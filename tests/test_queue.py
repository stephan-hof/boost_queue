import time
import threading
from unittest2 import TestCase

from boost_queue import Queue
from boost_queue import Full
from boost_queue import Empty

import Queue as std_queue

class TestQueue(TestCase):
    def test_put(self):
        return
        q = Queue(1)
        q.put(None)

    def test_put_with_full_error(self):
        return
        q = Queue(1)
        q.put(None)
        with self.assertRaises(Full):
            q.put(None, True, 2.1)

    def test_get_with_empty_error(self):
        return
        q = Queue(1)
        with self.assertRaises(Empty):
            q.get(1, 0.1)

    def test_get_put(self):
        return
        q = Queue(2)
        q.put(1, 1)
        q.put(2, 1)

        with self.assertRaises(Full):
            q.put(None, 1, 1)

        self.assertEqual(1, q.get())
        self.assertEqual(2, q.get())

        with self.assertRaises(Empty):
            q.get(1, 1)

    def test_get_put_with_thread_and_late_get(self):
        return
        def producer(q):
            [q.put(x, True, 0.1) for x in range(400)]

        def consumer(test, q):
            for x in range(400):
                self.assertEqual(x, q.get(True, 0.1))


        queue = Queue(400)
        t1 = threading.Thread(target=producer, args=(queue,))
        t1.start()
        time.sleep(0.2)
        t2 = threading.Thread(target=consumer, args=(self, queue))
        t2.start()

        t1.join()
        t2.join()

    def test_get_put_with_thread_and_late_put(self):
        return
        def consumer(test, q):
            to_consume = range(40)
            for x in range(40):
                to_consume.remove(q.get(True, 4))
            self.assertEqual(to_consume, [])

        def producer(q):
            for x in range(40):
                q.put(x, True, 0.1)

        queue = Queue(40)
        t1 = threading.Thread(target=consumer, args=(self, queue))
        t1.start()
        time.sleep(1)
        t2 = threading.Thread(target=producer, args=(queue,))
        t2.start()

        t1.join()
        t2.join()

    def test_unrealistic_max_size(self):
        with self.assertRaises(OverflowError):
            Queue(2**72)

        with self.assertRaises(OverflowError):
            Queue(-2**72)

    def test_negative_max_size(self):
        q = Queue(-1000)
        self.assertEqual(q.maxsize, 0)

    def test_maxsize_get(self):
        q = Queue(100)
        self.assertEqual(q.maxsize, 100)

    def test_unrealistic_timeout(self):
        q = Queue()
        with self.assertRaises(OverflowError):
            q.get(True, 2**72)

        with self.assertRaises(OverflowError):
            q.put(1, True, 2**72)

    def test_negative_timeout(self):
        q = Queue()
        with self.assertRaises(ValueError):
            q.get(True, -1)

        with self.assertRaises(ValueError):
            q.put('data', True, -1)

    def test_except_with_std_queue_full(self):
        q = Queue(1)
        q.put(1)
        with self.assertRaises(std_queue.Full):
            q.put(1, block=False)

        with self.assertRaises(Full):
            q.put(1, block=False)

    def test_except_with_std_queue_empty(self):
        q = Queue(1)
        with self.assertRaises(std_queue.Empty):
            q.get(block=False)

        with self.assertRaises(Empty):
            q.get(block=False)
