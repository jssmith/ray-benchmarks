import ray
import os
import sys
import hashlib
import random
import numpy as np

from collections import defaultdict
from random import shuffle
from utils import Timer, chunks, transpose
from sweep import sweep_iterations
import event_stats

key_len = 10
queries_per_split = 100

def usage():
    print "Usage: kvs_ray.py num_workers num_splits inputfile [inputfile ...]"

@ray.remote
def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

@ray.remote
def hash_split(input, num_splits):
    print "split input of length", len(input)
    splits = defaultdict(list)
    for line in input:
        hash_value = hash(line[:key_len])
        # print hash_value % num_splits
        splits[hash_value % num_splits].append(line)
    return [splits[i] for i in range(num_splits)]

def dict_merge(x, y):
    all_keys = set(x.keys()) | set(y.keys())
    res = {}
    for key in all_keys:
        key_sum = 0
        if key in x:
            key_sum += x[key]
        if key in y:
            key_sum += y[key]
        res[key] = key_sum
    return res

@ray.remote
def merge_hashed(input_splits):
    res = dict([(line[:key_len], line) for input in input_splits for line in input])
    print "have dict of size", len(res)
    return res

@ray.remote
def sample_input(input, frac):
    num_samples = int(frac * len(input))
    random.seed(hash(input[0]))
    shuffle(input)
    samples = np.empty(num_samples, '|S10')
    for i in range(num_samples):
        samples[i] = input[i][:10]
    return samples


@ray.remote
def _lookup(key, kvs_block):
    # t = Timer('lookup')
    ret = kvs_block[key]
    # t.finish()
    return ret

def lookup(key, kvs_blocks):
    hash_value = hash(key)
    num_splits = len(kvs_blocks)
    return _lookup.remote(key, kvs_blocks[hash_value % num_splits])

@ray.remote
def query(query_keys, kvs_blocks):
    t = Timer('query')
    ct = 0
    sumlen = 0
    t_loop = Timer('query-loop')
    for key in query_keys:
        sumlen += len(ray.get(lookup(key, kvs_blocks)))
        ct += 1
    t_loop.finish(queries_per_split)
    print "lookup count is", ct
    print "total length is", sumlen
    t.finish()
    return (ct, sumlen)

def benchmark_kvs(num_splits, input_files):
    with event_stats.benchmark_init():
        inputs = [load_files.remote(chunk_files) for chunk_files in chunks(input_files, num_splits)]
        [ray.wait([input]) for input in inputs]

        hs = [hash_split.remote(input, num_splits) for input in inputs]
        kvs_blocks = map(merge_hashed.remote, transpose([ray.get(s) for s in hs]))

        [ray.wait([b]) for b in kvs_blocks]

        # finish setting up kvs - get each key to make sure it is here
        input_samples = [sample_input.remote(input, .01) for input in inputs]
        query_keys = np.concatenate([ray.get(input_sample) for input_sample in input_samples])

    with event_stats.benchmark_measure():
        queries = [query.remote(query_keys, kvs_blocks) for chunk_files in chunks(input_files, num_splits)]
        [ray.wait([q]) for q in queries]


if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    if num_workers < 2 * num_splits:
        print 'require num_workers >= 2* num_splits'
        sys.exit(1)
    input_files = sys.argv[3:]
    if 'REDIS_ADDRESS' in os.environ:
        address_info = ray.init(redis_address=os.environ['REDIS_ADDRESS'])
    else:
        address_info = ray.init(start_ray_local=True, num_workers=num_workers)
    for _ in range(sweep_iterations):
        benchmark_kvs(num_splits, input_files)
    ray.flush_log()
    config_info = {
        'benchmark_name' : 'kvs',
        'benchmark_implementation' : 'ray',
        'benchmark_iterations' : sweep_iterations,
        'num_workers' : num_workers,
        'num_splits' : num_splits,
        'input_file_base' : input_files[0],
        'num_inputs' : len(input_files)
    }
    event_stats.print_stats_summary(config_info, address_info['redis_address'])
