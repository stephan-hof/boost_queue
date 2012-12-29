Introduction
============

boost_queue.cpp contains a queue class which follows the API from Queue.Queue of
the Python stdlib. The main difference is how the underlying locking is done. In
Python-2.X Queue.Queue uses a busy loop in case of a blocking operation.
This queue implementation uses condition variables from Boost to avoid the busy
loop.

boost_queue provides two methods not supported by the Queue from the stdlib.

put_many(items, block=True, timeout=None)
Instead of pushing a single item a list of items is pushed to the Queue atomicly.
'items' needs to support __len__ and __iter__ for this call.
If block equals 'True' the call blocks until enough free space is available to
put all items at once.

get_many(items, block=True, timeout=None)
Instead of returning a single item a tuple of items is returned atomicly.
Whereas 'items' is the  number of items this tuple should contain.
If block equals 'True' the call blocks until enough items are in the Queue.

The main usage of this calls are applications where the Queue is heavily used.

concurrent_queue.hpp contains a Python independent C++ Queue.

Changelog
=========

0.4.2 - December 29, 2012
------------------------

* Fix memory leak in get_many (Appears only if the timeout is reached)

0.4.1 - October 09, 2012
------------------------

* Fix memory leak in get_many
* Fix segfault if block and timeout are used

0.4 - March 14, 2012
--------------------

* add a get_many method
* add a put_many method

0.3 - March 03, 2012
--------------------

* Fix a memory leak
* Release the GIL less often. Now the GIL is only released if the queue needs to wait.
  In the versions before the GIL was released on *every* get/put operation which led
  to a lot of unneeded context switches. This change leads to a significant
  performance improvement if you work heavily on the queue.

0.2 - February 27, 2012
-----------------------

Let boost_queue.Empty and boost_queue.Full exceptions inhert from Queue.Empty and
Queue.Full. This allows other code to pass around the boost_queue.Queue object without
too many changes in old code.

0.1 - February 02, 2012
----------------------

- Initial release
