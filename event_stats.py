import redis
import json
import gzip
from collections import defaultdict
from math import sqrt
import time
import socket

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

def bytestohex(key):
    return ''.join([c.encode('hex') for c in key])

def build_event(key, value):
    (timestamp, event_type, status, extras) = value
    worker_id = bytestohex(key[10:30])
    task_id = bytestohex(key[31:51])
    return {
        'worker_id' : worker_id,
        'task_id' : task_id,
        'timestamp' : timestamp,
        'event_type' : event_type,
        'status' : status,
        'extras' : extras
    }

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
        event_key = (e['worker_id'], e['task_id'], e['event_type'])
        if e['event_type'].startswith('benchmark:'):
            if e['status'] == 1:
                self._benchmark_phase = e['event_type'][10:]
                self._benchmark_phase_start_timestamp = e['timestamp']
                self._start(e['timestamp'], event_key)
            elif e['status'] == 2:
                self._finish(e['timestamp'], event_key, e['event_type'])
                self._benchmark_phase = None
                self._benchmark_phase_start_timestamp = e['timestamp']
                self._benchmark_phase_id += 1
            else:
                raise RuntimeError("unknown status {}".format(e['status']))    
        else:
            if e['status'] == 1:
                self._start(e['timestamp'], event_key)
            elif e['status'] == 2:
                self._finish(e['timestamp'], event_key, e['event_type'])
            else:
                print "skipping event with unknown status {}".format(e['status'], str(e))

    def print_summary(self):
        for k, v in sorted(self._durations.items()):
            print k, " ", v

    def summary_stats(self):
        return dict(map(lambda (k,v): (k, v.summary()), self._durations.items()))


def benchmark_init():
    return RayLogSpan('benchmark:init')

def benchmark_measure():
    return RayLogSpan('benchmark:measure')

def get_worker_ips(r):
    worker_ips = {}
    for worker_key in r.keys("Workers:*"):
        node_ip_address = r.hget(worker_key, 'node_ip_address')
        worker_id = bytestohex(worker_key[8:29])
        worker_ips[worker_id] = node_ip_address
    return worker_ips


def read_stats(redis_address):
    # sleep briefly to allow driver writes to redis to propagate
    time.sleep(2)

    (host, port) = redis_address.split(':')
    port = int(port)
    r = redis.StrictRedis(host, port)

    worker_ips = get_worker_ips(r)
    worker_ips['DRIVER'] = socket.gethostbyname(socket.gethostname())


    all_events = [build_event(key, event_data) for key in r.keys('event*') for lrange in r.lrange(key, 0, -1) for event_data in json.loads(lrange)]
    all_events.sort(key=lambda x: x['timestamp'])
    dump = {
        'events' : all_events,
        'worker_ips' : worker_ips
    }
    with gzip.open("events.json.gz", "w") as f:
        json.dump(dump, f)
    print "number of events is", len(all_events)
    a = Analysis()
    for event in all_events:
        a.add_event(event)
    return a.summary_stats()

def print_stats_summary(config_info, redis_address):
    stats = {}
    stats['config'] = config_info
    stats['timing'] = read_stats(redis_address)
    print "BENCHMARK_STATS:", json.dumps(stats)


_events = []

class BenchmarkLogSpan():
    def __init__(self, name):
        self._name = name

    def _event(self, status):
        _events.append({
                'timestamp' : time.time(),
                'worker_id' : -1,
                'task_id' : -1,
                'event_type' : self._name,
                'status' : status,
                'extras' : {},
            })

    def __enter__(self):
        self._event(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._event(2)

def benchmark_init_noray():
    return BenchmarkLogSpan('benchmark:init')

def benchmark_measure_noray():
    return BenchmarkLogSpan('benchmark:measure')

def print_stats_summary_noray(config_info):
    stats = {}
    stats['config'] = config_info
    a = Analysis()
    for event in _events:
        a.add_event(event)
    stats['timing'] = a.summary_stats()
    print "BENCHMARK_STATS:", json.dumps(stats)
