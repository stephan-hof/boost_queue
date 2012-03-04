from boost_queue import Queue
#from Queue import Queue
import time
import threading

## boost_queue (nb_threads, seconds)
# 1 => 5.50
# 2 => 10.30
# 3 => 14.60

## std-Queue
# 1 => 22.61
# 2 => 45.24
# 3 => 76.62

class BigFatObject(object):
    def __init__(self):
        self.a = 'adsfadsfadfs'
        self.b = 'xxxxx'
        self.c = {}

class Con(threading.Thread):
    def __init__(self, q):
        self.q = q
        threading.Thread.__init__(self)

    def run(self):
        for _ in xrange(10**6):
            self.q.get()

class Prod(threading.Thread):
    def __init__(self, q):
        self.q = q
        threading.Thread.__init__(self)

    def run(self):
        for _ in xrange(10**6):
            self.q.put(BigFatObject())

if __name__ == '__main__':
    q = Queue()
    nb_threads = 1
    cons = [Con(q) for _ in range(nb_threads)]
    prods = [Prod(q) for _ in range(nb_threads)]

    start = time.time()
    [x.start() for x in cons + prods]
    [x.join() for x in prods + cons]
    print time.time() - start
