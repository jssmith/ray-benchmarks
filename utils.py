import time

def chunks(l, n):
    chunk_size = (len(l) - 1) / n + 1
    for i in xrange(0, len(l), chunk_size):
        yield l[i:i + chunk_size]

class Timer(object):
    def __init__(self, name, ct=None):
        self._name = name
        self._start_time = time.time()
        self._ct = ct

    def finish(self):
        elapsed_time = time.time() - self._start_time
        if self._ct is not None:
            rate_str = " - {} per sec".format(self._ct / elapsed_time)
        else:
            rate_str = ""
        print '{}: {:.6f}{}'.format(self._name, elapsed_time, rate_str)

