import redis
import json
from collections import defaultdict
from math import sqrt

from ray.worker import RayLogSpan

class BasicStatistics(object):
    def __init__(self):
        self.ct = 0
        self.total_elapsed_time = 0
        self._total_elapsed_time_sq = 0
        self.min_elapsed_time = None
        self.max_elapsed_time = None

    def add(self, elapsed_time):
        self.ct += 1
        self.total_elapsed_time += elapsed_time
        self._total_elapsed_time_sq += elapsed_time * elapsed_time
        
        if self.min_elapsed_time is None or self.min_elapsed_time > elapsed_time:
            self.min_elapsed_time = elapsed_time
        if self.max_elapsed_time is None or self.max_elapsed_time < elapsed_time:
            self.max_elapsed_time = elapsed_time

    def avg_time(self):
        return float(self.total_elapsed_time) / float(self.ct)

    def stddev_time(self):
        s = float(self.total_elapsed_time)
        ss = float(self._total_elapsed_time_sq)
        n = float(self.ct)
        return sqrt(ss/n - (s/n) ** 2)

    def summary(self):
        return { 'ct' : self.ct,
                'total_elapsed_time': self.total_elapsed_time,
                'min_elapsed_time' : self.min_elapsed_time,
                'max_elapsed_time' : self.max_elapsed_time,
                'avg_elapsed_time' : self.avg_time(),
                'stddev_elapsed_time' : self.stddev_time()
            }

    def __str__(self):
        return '{},{},[{},{},{}],{}'.format(self.total_elapsed_time, self.ct, self.min_elapsed_time, self.avg_time(), self.max_elapsed_time, self.stddev_time())

class Event(object):
    def __init__(self, key, value):
        self.key = key
        (self.timestamp, self.event_type, self.status, self.extras) = value

class Analysis(object):
    def __init__(self):
        self._start_times = {}
        self._durations = defaultdict(BasicStatistics)
        self._benchmark_phase = None
        self._benchmark_phase_start_timestamp = None
        self._benchmark_phase_id = 0

    def _start(self, start_timestamp, key):
        if key in self._start_times:
            raise RuntimeError("event already started {}".format(event_type))
        self._start_times[key] = (start_timestamp, self._benchmark_phase, self._benchmark_phase_id)

    def _finish(self, end_timestamp, key, event_type):
        (start_timestamp, start_phase, start_phase_id) = self._start_times[key]
        end_phase = self._benchmark_phase
        end_phase_id = self._benchmark_phase_id
        elapsed_time = end_timestamp - start_timestamp
        del self._start_times[key]

        # if less than 10% of the execution slipped into a different end phase then
        # categorize it with the start phase
        if end_phase != start_phase and end_phase_id == start_phase_id + 1:
            current_phase_elapsed_time = end_timestamp - self._benchmark_phase_start_timestamp
            if current_phase_elapsed_time / elapsed_time < 0.1:
                end_phase = start_phase
        
        self._durations['{}:{}:{}'.format(start_phase, end_phase, event_type)].add(elapsed_time)

    def add_event(self, e):
        event_key = (e.key, e.event_type)
        if e.event_type.startswith('benchmark:'):
            if e.status == 1:
                self._benchmark_phase = e.event_type[10:]
                self._benchmark_phase_start_timestamp = e.timestamp
                self._start(e.timestamp, event_key)
            elif e.status == 2:
                self._finish(e.timestamp, event_key, e.event_type)
                self._benchmark_phase = None
                self._benchmark_phase_start_timestamp = e.timestamp
                self._benchmark_phase_id += 1
            else:
                raise RuntimeError("unknown status {}".format(s.status))    
        else:
            if e.status == 1:
                self._start(e.timestamp, event_key)
            elif e.status == 2:
                self._finish(e.timestamp, event_key, e.event_type)
            else:
                raise RuntimeError("unknown status {}".format(s.status))
            # print e.event_type

    def print_summary(self):
        for k, v in sorted(self._durations.items()):
            print k, " ", v

    def summary_stats(self):
        return dict(map(lambda (k,v): (k, v.summary()), self._durations.items()))


def benchmark_init():
    return RayLogSpan('benchmark:init')

def benchmark_measure():
    return RayLogSpan('benchmark:measure')

def read_stats(redis_address):
    (host, port) = redis_address.split(':')
    port = int(port)
    r = redis.StrictRedis(host, port)
    all_events = [Event(key, event_data) for key in r.keys('event*') for event_data in json.loads(r.lrange(key, 0, -1)[0])]
    print "number of events is", len(all_events)
    all_events.sort(key=lambda x: x.timestamp)
    a = Analysis()
    for event in all_events:
        a.add_event(event)
    return a.summary_stats()

def print_stats_summary(config_info, redis_address):
    stats = {}
    stats['config'] = config_info
    stats['timing'] = read_stats(redis_address)
    print "BENCHMARK_STATS:", json.dumps(stats)
