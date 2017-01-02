import os
import time

def chunks(l, n):
    chunk_size = (len(l) - 1) / n + 1
    for i in xrange(0, len(l), chunk_size):
        yield l[i:i + chunk_size]

def transpose(listoflists):
    return map(list, zip(*listoflists))

class Timer(object):
    def __init__(self, name):
        self._name = name
        self._start_time = time.time()
        self._pid = os.getpid()
        print 'TM:{}:STRT:{:.6f} - {}'.format(self._pid, self._start_time, self._name)

    def finish(self, ct=None):
        end_time = time.time()
        elapsed_time = end_time - self._start_time
        print 'TM:{}:FNSH:{:.6f} - {}'.format(self._pid, end_time, self._name)
        if ct is not None:
            rate_str = " - {} per sec".format(ct / elapsed_time)
        else:
            rate_str = ""
        print '{}: {:.6f}{}'.format(self._name, elapsed_time, rate_str)

