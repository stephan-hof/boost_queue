/*
 * Copyright Stephan Hofmockel 2012.
 * Distributed under the Boost Software License, Version 1.0.
 * See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt
*/

#include "Python.h"

#include <exception>
#include <deque>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread_time.hpp>
#include <boost/foreach.hpp>

/* get_many considarations:
 * Avoid using get_many() on the consumer side and using put() on the producer.
 * The get_many() thread is notified on every single put(), however immediatly
 * goes to sleep again because the Queue is not big enough. Hence a lot of
 * context switches for nothing. So it is recommended to use get_many() and
 * put_many() together with the same size of items.
 */

/* Macro to wrap c++ exceptions into python ones */
#define BEGIN_SAFE_CALL try {
#define END_SAFE_CALL(error_txt1, ret_val) } \
    catch (std::exception &e) { \
        PyErr_Format(PyExc_Exception, error_txt1, e.what()); return ret_val;} \
    catch (...) { \
        PyErr_Format(PyExc_Exception, error_txt1, "unknown error"); return ret_val;}

/* Helper class to get or release the GIL in a exception safe manner */
class AllowThreads {
    private:
        PyThreadState *_save;
    public:
        AllowThreads(){Py_UNBLOCK_THREADS}
        ~AllowThreads(){Py_BLOCK_THREADS}
};

static const char *put_kwlist[] = {"item", "block", "timeout", NULL};
static const char *put_many_kwlist[] = {"items", "block", "timeout", NULL};
static const char *get_kwlist[] = {"block", "timeout", NULL};
static const char *get_many_kwlist[] = {"items", "block", "timeout", NULL};


static PyObject * EmptyError;
static PyObject * FullError;

class Bridge {
    public:
        boost::mutex mutex;
        boost::condition_variable empty_cond;
        boost::condition_variable full_cond;
        boost::condition_variable all_tasks_done_cond;
        std::deque<PyObject*> queue;
};

typedef struct {
    PyObject_HEAD
    Bridge *bridge;
    size_t maxsize;
    boost::uint64_t unfinished_tasks;
} Queue;


static PyObject *
Queue_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    Queue *self;
    self = reinterpret_cast<Queue*> (type->tp_alloc(type, 0));
    self->bridge = NULL;
    self->unfinished_tasks = 0;
    self->maxsize = 0;

    return reinterpret_cast<PyObject*>(self);
}

static int
Queue_init(Queue *self, PyObject *args, PyObject *kwargs)
{
    long int maxsize=0;
    if(!PyArg_ParseTuple(args, "|l", &maxsize))
        return -1;

    if (maxsize < 0) {
        self->maxsize = 0;
    }
    else {
        self->maxsize = maxsize;
    }

    BEGIN_SAFE_CALL
        self->bridge = new Bridge();
    END_SAFE_CALL("Error creating underlying queue: %s", -1)
    return 0;
}

static int
Queue_traverse(Queue *self, visitproc visit, void *arg)
{
    BEGIN_SAFE_CALL
        BOOST_FOREACH(PyObject* entry, self->bridge->queue) {
            Py_VISIT(entry);
        }
    END_SAFE_CALL("Error while traversing: %s", -1)
    return 0;
}

static int
Queue_clear(Queue *self)
{
    BEGIN_SAFE_CALL
        BOOST_FOREACH(PyObject* entry, self->bridge->queue) {
            Py_CLEAR(entry);
        }
    END_SAFE_CALL("Error while clear: %s", -1)
    return 0;
}

static void
Queue_dealloc(Queue *self)
{
    if (self->bridge) {
        Queue_clear(self);
        delete self->bridge;
    }
    self->ob_type->tp_free(reinterpret_cast<PyObject*>(self));
}


static int
_parse_block_and_timeout(
        PyObject *py_block,
        PyObject *py_timeout,
        bool & block,
        double & timeout)
{

    if (py_block != NULL and PyObject_Not(py_block)) {
        block = false;
    }

    /* timeout = None => only block is used
     * timeout < 0 => Value Error
     * timeout > 0 wait for timeout
     * timeout == 0 => block = false
     */
    if (py_timeout != NULL and py_timeout != Py_None) {
        timeout = PyFloat_AsDouble(py_timeout);
        if (PyErr_Occurred()) {
            PyErr_Format(PyExc_ValueError, "'timeout' is not a valid float");
            return -1;
        }

        if (timeout < 0) {
            PyErr_Format(PyExc_ValueError, "'timeout' must be positive");
            return -1;
        }

        if (timeout > static_cast<double>(std::numeric_limits<time_t>::max())) {
            PyErr_Format(PyExc_OverflowError, "timeout is too large");
            return -1;
        }

        if (timeout == 0) {
            block = false;
        }
    }
    return 1;
}

static void
_wait_for_lock(boost::mutex::scoped_lock& lock)
{
    AllowThreads raii_lock;
    lock.lock();
}

static void
_blocked_wait_full(Bridge* bridge, boost::mutex::scoped_lock& lock)
{
    AllowThreads raii_lock;
    bridge->full_cond.wait(lock);
}

static bool
_timed_wait_full(
        Bridge* bridge,
        boost::mutex::scoped_lock& lock,
        boost::system_time& timeout)
{
    AllowThreads raii_lock;
    return bridge->full_cond.timed_wait(lock, timeout);
}

static bool
_wait_for_free_slots(
        Queue *self,
        bool block,
        double timeout,
        boost::mutex::scoped_lock& lock,
        size_t nb_of_items)
{
    boost::uint64_t timeout_millis = static_cast<boost::uint64_t>(timeout*1000);

    if(self->maxsize == 0) {
        /* Fall through the end of method */
    }
    else if((self->maxsize - self->bridge->queue.size()) >= nb_of_items) {
        /* Fall through the end of method */
    }
    else if (not block) {
        PyErr_Format(FullError, "Queue Full");
        return false;
    }
    else if (timeout > 0) {
        boost::system_time abs_timeout = boost::get_system_time();
        abs_timeout += boost::posix_time::milliseconds(timeout_millis);
        while (!((self->maxsize - self->bridge->queue.size()) >= nb_of_items)) {
            if (not _timed_wait_full(self->bridge, lock, abs_timeout)) {
                PyErr_Format(FullError, "Queue Full");
                return false;
            }
        }
    }
    else {
        while (!((self->maxsize - self->bridge->queue.size()) >= nb_of_items)) {
            _blocked_wait_full(self->bridge, lock);
        }
    }
    return true;
}

static PyObject*
_internal_put(Queue *self, PyObject *item, bool block, double timeout)
{
    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    if (not _wait_for_free_slots(self, block, timeout, lock, 1)) {
        return NULL;
    }

    self->bridge->queue.push_back(item);
    Py_INCREF(item);

    self->unfinished_tasks += 1;
    self->bridge->empty_cond.notify_one();

    END_SAFE_CALL("Error in put: %s", NULL)
    Py_RETURN_NONE;
}

static PyObject*
Queue_put(Queue *self, PyObject *args, PyObject *kwargs)
{
    PyObject *item;

    PyObject *py_block=NULL;
    bool block=true;

    PyObject *py_timeout=NULL;
    double timeout = 0;
    
    if (not PyArg_ParseTupleAndKeywords(
                                args,
                                kwargs,
                                "O|OO:put",
                                const_cast<char**>(put_kwlist),
                                &item,
                                &py_block,
                                &py_timeout))
    {
        return NULL;
    }

    if (_parse_block_and_timeout(py_block, py_timeout, block, timeout) == -1) {
        return NULL;
    }
    return _internal_put(self, item, block, timeout);
}

static PyObject*
Queue_put_many(Queue *self, PyObject *args, PyObject *kwargs)
{
    PyObject *items;

    PyObject *py_block=NULL;
    bool block=true;

    PyObject *py_timeout=NULL;
    double timeout = 0;

    PyObject *iterator=NULL;
    PyObject *itertor_item=NULL;
    Py_ssize_t items_len;

    if (not PyArg_ParseTupleAndKeywords(
                                args,
                                kwargs,
                                "O|OO:put",
                                const_cast<char**>(put_many_kwlist),
                                &items,
                                &py_block,
                                &py_timeout))
    {
        return NULL;
    }

    if (_parse_block_and_timeout(py_block, py_timeout, block, timeout) == -1) {
        return NULL;
    }

    if ((items_len = PyObject_Length(items)) == -1) {
        return NULL;
    }

    if (items_len == 0) {
        Py_RETURN_NONE;
    }

    /* Can happen if items is a custom object overloading __len__ */
    if (items_len < 0) {
        return PyErr_Format(
                PyExc_ValueError,
                "len of items is smaller 0: %i",
                items_len);
    }

    if (self->maxsize > 0 and static_cast<size_t>(items_len) > self->maxsize) {
        return PyErr_Format(
                    PyExc_ValueError,
                    "items of size %i is bigger than maxsize: %i",
                    items_len,
                    self->maxsize);
    }

    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    if (not _wait_for_free_slots(self, block, timeout, lock, static_cast<size_t>(items_len))) {
        return NULL;
    }

    if ((iterator = PyObject_GetIter(items)) == NULL) {
        return NULL;
    }

    while ((itertor_item = PyIter_Next(iterator))) {
        self->bridge->queue.push_back(itertor_item);
        self->unfinished_tasks += 1;

    }
    self->bridge->empty_cond.notify_all();
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        return NULL;
    }

    END_SAFE_CALL("Error in put_many: %s", NULL)
    Py_RETURN_NONE;
}

static void
_blocked_wait_empty(Bridge* bridge, boost::mutex::scoped_lock& lock)
{
    AllowThreads raii_lock;
    bridge->empty_cond.wait(lock);
}

static bool
_timed_wait_empty(
        Bridge* bridge,
        boost::mutex::scoped_lock& lock,
        boost::system_time& timeout)
{
    AllowThreads raii_lock;
    return bridge->empty_cond.timed_wait(lock, timeout);
}

static bool
_wait_for_items(
        Queue *self,
        bool block,
        double timeout,
        boost::mutex::scoped_lock& lock,
        long int items_len)
{
    boost::uint64_t timeout_millis = static_cast<boost::uint64_t>(timeout*1000);

    if (self->bridge->queue.size() >= static_cast<size_t>(items_len)) {
        /* Fall through the end of method */
    }
    else if (not block) {
        PyErr_Format(EmptyError, "Queue Empty");
        return false;
    }
    else if (timeout > 0) {
        boost::system_time abs_timeout = boost::get_system_time();
        abs_timeout += boost::posix_time::milliseconds(timeout_millis);
        while (self->bridge->queue.size() < items_len) {
            if (not _timed_wait_empty(self->bridge, lock, abs_timeout)) {
                PyErr_Format(EmptyError, "Queue Empty");
                return false;
            }
        }
    }
    else {
        while (not (self->bridge->queue.size() >= static_cast<size_t>(items_len))) {
            _blocked_wait_empty(self->bridge, lock);
        }
    }
    return true;
}

static PyObject*
_internal_get(Queue *self, bool block, double timeout)
{
    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    if (not _wait_for_items(self, block, timeout, lock, 1)) {
        return NULL;
    }

    PyObject *item = self->bridge->queue.front();
    self->bridge->queue.pop_front();
    self->bridge->full_cond.notify_one();
    return item;

    END_SAFE_CALL("Error in get: %s", NULL)
}

static PyObject*
Queue_get(Queue *self, PyObject *args, PyObject *kwargs)
{

    PyObject *py_block=NULL;
    PyObject *py_timeout=NULL;

    bool block=true;
    double timeout = 0;
    
    if(not PyArg_ParseTupleAndKeywords(
                                args,
                                kwargs,
                                "|OO:get",
                                const_cast<char**>(get_kwlist),
                                &py_block,
                                &py_timeout))
    {
        return NULL;
    }

    if (_parse_block_and_timeout(py_block, py_timeout, block, timeout) == -1) {
        return NULL;
    }
    return _internal_get(self, block, timeout);
}

static PyObject*
Queue_get_many(Queue *self, PyObject *args, PyObject *kwargs)
{

    PyObject *py_block=NULL;
    PyObject *py_timeout=NULL;
    PyObject *result_tuple=NULL;

    bool block=true;
    double timeout = 0;
    long int items=0;

    if(not PyArg_ParseTupleAndKeywords(
                                args,
                                kwargs,
                                "l|OO:get",
                                const_cast<char**>(get_many_kwlist),
                                &items,
                                &py_block,
                                &py_timeout))
    {
        return NULL;
    }

    if (_parse_block_and_timeout(py_block, py_timeout, block, timeout) == -1) {
        return NULL;
    }

    if (items == 0) {
        return PyTuple_New(0);
    }

    if (items < 0) {
        return PyErr_Format(
                PyExc_ValueError,
                "items must be greater or equal 0 but it is: %ld",
                items);
    }


    if (self->maxsize > 0 and static_cast<size_t>(items) > self->maxsize) {
        return PyErr_Format(
                PyExc_ValueError,
                "you want to get %ld but maxsize is %i",
                items,
                self->maxsize);
    }

    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    if (not _wait_for_items(self, block, timeout, lock, items)) {
        return NULL;
    }

    if ((result_tuple = PyTuple_New(items)) == NULL) {
        return NULL;
    }

    for (long int i=0; i<items; i++) {
        PyObject *item = self->bridge->queue.front();
        PyTuple_SET_ITEM(result_tuple, i, item);

        self->bridge->queue.pop_front();
    }

    self->bridge->full_cond.notify_all();
    return result_tuple;


    END_SAFE_CALL("Error in get_many: %s", NULL)
}

static PyObject*
Queue_qsize(Queue *self)
{
    return PyLong_FromSize_t(self->bridge->queue.size());
}

static PyObject*
Queue_empty(Queue *self)
{
    if (self->bridge->queue.size() == 0) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject*
Queue_full(Queue *self)
{
    if (self->maxsize == 0) {
        Py_RETURN_FALSE;
    }

    if (self->bridge->queue.size() < self->maxsize) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

static PyObject*
Queue_put_nowait(Queue *self, PyObject *item)
{
    return _internal_put(self, item, false, 0);
}

static PyObject*
Queue_get_nowait(Queue *self)
{
    return _internal_get(self, false, 0);
}

static PyObject*
Queue_task_done(Queue *self)
{
    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    if (self->unfinished_tasks == 0) {
        return PyErr_Format(
                    PyExc_ValueError, "task_done() called too many times");
    }

    if (--self->unfinished_tasks == 0) {
        self->bridge->all_tasks_done_cond.notify_all();
    }

    END_SAFE_CALL("Error in task_done: %s", NULL)
    Py_RETURN_NONE;
}

static void
_blocked_wait_all_tasks_done(Queue* self, boost::mutex::scoped_lock& lock)
{
    AllowThreads raii_lock;
    self->bridge->all_tasks_done_cond.wait(lock);
}

static PyObject*
Queue_join(Queue* self)
{
    BEGIN_SAFE_CALL

    boost::mutex::scoped_lock lock(self->bridge->mutex, boost::try_to_lock);
    if (not lock.owns_lock()) {
        _wait_for_lock(lock);
    }

    while (self->unfinished_tasks) {
        _blocked_wait_all_tasks_done(self, lock);
    }

    END_SAFE_CALL("Error in join: %s", NULL)
    Py_RETURN_NONE;
}

static PyMethodDef Queue_methods[] = {
    {"put", (PyCFunction)Queue_put, METH_VARARGS|METH_KEYWORDS, ""},
    {"get", (PyCFunction)Queue_get, METH_VARARGS|METH_KEYWORDS, ""},
    {"qsize", (PyCFunction)Queue_qsize, METH_NOARGS, ""},
    {"empty", (PyCFunction)Queue_empty, METH_NOARGS, ""},
    {"full", (PyCFunction)Queue_full, METH_NOARGS, ""},
    {"put_nowait", (PyCFunction)Queue_put_nowait, METH_O, ""},
    {"get_nowait", (PyCFunction)Queue_get_nowait, METH_NOARGS, ""},
    {"put_many", (PyCFunction)Queue_put_many, METH_VARARGS|METH_KEYWORDS, ""},
    {"get_many", (PyCFunction)Queue_get_many, METH_VARARGS|METH_KEYWORDS, ""},
    {"task_done", (PyCFunction)Queue_task_done, METH_NOARGS, ""},
    {"join", (PyCFunction)Queue_join, METH_NOARGS, ""},
    {NULL, NULL, 0, NULL}
};

static PyObject *
Queue_maxsize_get(Queue *self, void *closure)
{
    return PyLong_FromSize_t(self->maxsize);
}

static PyGetSetDef Queue_getsets[] = {
    {const_cast<char*>("maxsize"), (getter)Queue_maxsize_get, NULL, const_cast<char*>(""), NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject QueueType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "boost_queue.Queue",       /*tp_name*/
    sizeof(Queue),             /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Queue_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,/*tp_flags*/
    "",                        /* tp_doc */
    (traverseproc)Queue_traverse,   /* tp_traverse */
    (inquiry)Queue_clear,           /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Queue_methods,             /* tp_methods */
    0,                         /* tp_members */
    Queue_getsets,             /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Queue_init,      /* tp_init */
    0,                         /* tp_alloc */
    Queue_new,                 /* tp_new */
};

PyMODINIT_FUNC
initboost_queue(void){
    PyObject* module;
    PyObject* std_lib_queue;
    PyObject* std_empty;
    PyObject* std_full;

    if (PyType_Ready(&QueueType) < 0) {
        return;
    }

    module = Py_InitModule("boost_queue", NULL);
    if (module == NULL) {
        return;
    }

    if((std_lib_queue = PyImport_ImportModule("Queue")) == NULL) {
        return;
    }

    if((std_empty = PyObject_GetAttrString(std_lib_queue, "Empty")) == NULL) {
        Py_DECREF(std_lib_queue);
    }

    if((std_full = PyObject_GetAttrString(std_lib_queue, "Full")) == NULL) {
        Py_DECREF(std_lib_queue);
        Py_DECREF(std_empty);
    }


    EmptyError = PyErr_NewException(
                            const_cast<char*>("boost_queue.Empty"),
                            std_empty,
                            NULL);

    FullError = PyErr_NewException(
                            const_cast<char*>("boost_queue.Full"),
                            std_full,
                            NULL);

    Py_DECREF(std_lib_queue);
    Py_DECREF(std_empty);
    Py_DECREF(std_full);

    PyModule_AddObject(module, "Empty", EmptyError);
    PyModule_AddObject(module, "Full", FullError);

    Py_INCREF((PyObject*) &QueueType);
    PyModule_AddObject(module, "Queue", (PyObject*)&QueueType);
}
