/*
 * Copyright Stephan Hofmockel 2012.
 * Distributed under the Boost Software License, Version 1.0.
 * See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt
*/

#include "Python.h"
#include <exception>
#include <deque>
#include <limits>
#include <ctime>
#include <boost/cstdint.hpp>
#include <boost/thread/mutex.hpp>
#include "concurrent_queue.hpp"

static const char *put_kwlist[] = {"item", "block", "timeout", NULL};
static const char *get_kwlist[] = {"block", "timeout", NULL};

static PyObject * EmptyError;
static PyObject * FullError;

class PyObjectQueue: public ConcurrentQueue<PyObject*>{
    public:
        PyObjectQueue(long &);
        int py_traverse(visitproc visit, void *arg);
        int py_clear();
};

PyObjectQueue::PyObjectQueue(long &maxsize): ConcurrentQueue<PyObject*>(maxsize) {}

int
PyObjectQueue::py_traverse(visitproc visit, void *arg)
{
    boost::lock_guard<boost::mutex> raii_lock(this->mutex);

    std::deque<PyObject*>::iterator iter;
    for (iter = this->queue.begin(); iter != this->queue.end(); iter++) {
        Py_VISIT((*iter));
    }
    return 0;
}

int
PyObjectQueue::py_clear()
{
    boost::lock_guard<boost::mutex> raii_lock(this->mutex);

    std::deque<PyObject*>::iterator iter;
    for (iter = this->queue.begin(); iter != this->queue.end(); iter++) {
        Py_CLEAR((*iter));
    }
    return 0;
}
typedef struct 
{
    PyObject_HEAD
    PyObjectQueue *queue;

} Queue;

static PyObject *
Queue_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    Queue *self;
    self = reinterpret_cast<Queue*> (type->tp_alloc(type, 0));
    self->queue = NULL;
    return reinterpret_cast<PyObject*>(self);
}

static int
Queue_init(Queue *self, PyObject *args, PyObject *kwargs)
{
    long int maxsize=0;
    if(!PyArg_ParseTuple(args, "|l", &maxsize))
        return -1;

    try {
        self->queue = new PyObjectQueue(maxsize);
    }
    catch (std::exception &e) {
        PyErr_Format(
            PyExc_Exception,
            "Error while creating underlying queue: %s", e.what());
        return -1;
    }
    catch (...) {
        PyErr_Format(PyExc_Exception, "Unkown error while creating queue");
        return -1;
    }
    return 0;
}

static int
Queue_traverse(Queue *self, visitproc visit, void *arg)
{
    try {
        return self->queue->py_traverse(visit, arg);
    }
    catch (...) {
        PyErr_Format(PyExc_Exception, "Error while traversing");
        return -1;
    }
}

static int
Queue_clear(Queue *self)
{
    try {
        return self->queue->py_clear();
    }
    catch (...) {
        PyErr_Format(PyExc_Exception, "Error while clear");
        return -1;
    }
}

static void
Queue_dealloc(Queue *self)
{
    if (self->queue) {
        Queue_clear(self);
        delete self->queue;
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

static PyObject *
_internal_put(Queue *self, PyObject *item, bool block, double timeout)
{
    PyThreadState * _save;
    Py_UNBLOCK_THREADS
    try {
        self->queue->put(
            item,
            block,
            static_cast<boost::uint64_t>(timeout*1000));

        Py_BLOCK_THREADS
        Py_INCREF(item);
        Py_RETURN_NONE;
    }
    catch (QueueFull &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(FullError, "Queue Full");
    }
    catch (std::exception &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Error in put: %s", e.what());
    }
    catch (...) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Unkown error in put");
    }
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
_internal_get(Queue *self, bool block, double timeout)
{
    PyObject * ret_item;
    PyThreadState * _save;
    Py_UNBLOCK_THREADS
    try {
        self->queue->pop(
                ret_item,
                block,
                static_cast<boost::uint64_t>(timeout*1000));

        Py_BLOCK_THREADS
        Py_INCREF(ret_item);
        return ret_item;
    }
    catch (QueueEmpty &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(EmptyError, "Queue Empty");
    }
    catch (std::exception &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Error in get: %s", e.what());
    }
    catch (...) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Unkown error in get");
    }
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
Queue_qsize(Queue *self)
{
    return PyLong_FromSize_t(self->queue->size());
}

static PyObject*
Queue_empty(Queue *self)
{
    if (self->queue->size() == 0) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject*
Queue_full(Queue *self)
{
    if (self->queue->get_maxsize() == 0) {
        Py_RETURN_FALSE;
    }
    if (self->queue->size() < self->queue->get_maxsize()) {
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
    
    PyThreadState * _save;
    Py_UNBLOCK_THREADS

    try {
        self->queue->task_done(); 

        Py_BLOCK_THREADS
        Py_RETURN_NONE;
    }
    catch (NoMoreTasks &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(
                    PyExc_ValueError, "task_done() called too many times");
    }
    catch (std::exception &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(
                    PyExc_Exception, "Error in task_done: %s", e.what());
    }
    catch (...) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Unkown error in task_done");
    }
}

static PyObject*
Queue_join(Queue *self)
{
    
    PyThreadState * _save;
    Py_UNBLOCK_THREADS

    try {
        self->queue->join(); 

        Py_BLOCK_THREADS
        Py_RETURN_NONE;
    }
    catch (std::exception &e) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Error in join: %s", e.what());
    }
    catch (...) {
        Py_BLOCK_THREADS
        return PyErr_Format(PyExc_Exception, "Unkown error in join");
    }
}

static PyMethodDef Queue_methods[] = {
    {"put", (PyCFunction)Queue_put, METH_VARARGS|METH_KEYWORDS, ""},
    {"get", (PyCFunction)Queue_get, METH_VARARGS|METH_KEYWORDS, ""},
    {"qsize", (PyCFunction)Queue_qsize, METH_NOARGS, ""},
    {"empty", (PyCFunction)Queue_empty, METH_NOARGS, ""},
    {"full", (PyCFunction)Queue_full, METH_NOARGS, ""},
    {"put_nowait", (PyCFunction)Queue_put_nowait, METH_O, ""},
    {"get_nowait", (PyCFunction)Queue_get_nowait, METH_NOARGS, ""},
    {"task_done", (PyCFunction)Queue_task_done, METH_NOARGS, ""},
    {"join", (PyCFunction)Queue_join, METH_NOARGS, ""},
    {NULL, NULL, 0, NULL}
};

static PyObject *
Queue_maxsize_get(Queue *self, void *closure)
{
    return PyLong_FromSize_t(self->queue->get_maxsize());
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
