Introduction
============

boost_queue.cpp contains a queue class which follows the API from Queue.Queue of 
the Python stdlib. The main difference is how the underlying locking is done. In
Python-2.X Queue.Queue uses a busy loop in case of a blocking operation. 
This queue implementation uses condition variables from Boost to avoid the busy
loop.

concurrent_queue.hpp contains the Python independent C++ Queue

Changelog
=========

0.2 - February 27, 2012
-----------------------

Let boost_queue.Empty and boost_queue.Full exceptions inhert from Queue.Empty and
Queue.Full. This allows other code to pass around the boost_queue.Queue object without
too many changes in old code.

0.1 - February 02, 2012
----------------------

- Initial release
