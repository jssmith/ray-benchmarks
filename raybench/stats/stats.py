import argparse
import base64
import redis
import json
import sys
import gzip
import socket
import os
import time

from ray.worker import RayLogSpan

def benchmark_init():
    return RayLogSpan('benchmark:init')

def benchmark_measure():
    return RayLogSpan('benchmark:measure')

def benchmark_init_noray():
    return BenchmarkLogSpan('benchmark:init')

def benchmark_measure_noray():
    return BenchmarkLogSpan('benchmark:measure')

def save_ray_events(redis_address, config_info, sleep_delay=2):
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

    # sleep briefly to allow driver writes to redis to propagate
    time.sleep(sleep_delay)

    # connect to Redis
    read_start_time = time.time()
    (host, port) = redis_address.split(':')
    port = int(port)
    r = redis.StrictRedis(host, port)

    # get the worker ip addresses
    worker_ips = {}
    for worker_key in r.keys("Workers:*"):
        node_ip_address = r.hget(worker_key, 'node_ip_address')
        worker_id = bytestohex(worker_key[8:29])
        worker_ips[worker_id] = node_ip_address
    worker_ips['DRIVER'] = socket.gethostbyname(socket.gethostname())

    all_events = [build_event(key, event_data) for key in r.keys('event*') for lrange in r.lrange(key, 0, -1) for event_data in json.loads(lrange)]
    all_events.sort(key=lambda x: x['timestamp'])

    read_end_time = time.time()
    read_elapsed_time = read_end_time - read_start_time
    print "fetched events {} in {}".format(len(all_events), read_elapsed_time)        

    dump = {
        'config' : config_info,
        'events' : all_events,
        'worker_ips' : worker_ips
    }

    with gzip.open("benchmark_log.json.gz", "w") as f:
        json.dump(dump, f)


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

