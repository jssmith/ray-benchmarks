import ray
import sys
import hashlib
import random

from collections import defaultdict
from random import shuffle
from utils import Timer, chunks, transpose

key_len = 10
queries_per_split = 100

def usage():
    print "Usage: kvs_ray num_workers num_splits inputfile [inputfile ...]"

def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

@ray.remote
def load_files(input_files):
    res = [line for input_file in input_files for line in read_input(input_file)]
    print 'loaded {} : {}'.format(str(input_files), len(res))
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
def query(input_files, kvs_blocks):
    t = Timer('query')
    query_keys = [line[:key_len] for input_file in input_files for line in read_input(input_file)]
    random.seed(hash(input_files[0]))
    shuffle(query_keys)
    query_keys = query_keys[:queries_per_split]
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
    t_load = Timer('load')
    inputs = [load_files.remote(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    hs = [hash_split.remote(input, num_splits) for input in inputs]
    res = map(merge_hashed.remote, transpose([ray.get(s) for s in hs]))

    # finish setting up kvs
    [ray.get(r) for r in res]
    kvs_blocks = [r for r in res]
    t_load.finish()

    t_query = Timer('RAY_BENCHMARK_KVS')
    queries = [query.remote(chunk_files, kvs_blocks) for chunk_files in chunks(input_files, num_splits)]
    [ray.get(q) for q in queries]
    t_query.finish()

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
    ray.init(start_ray_local=True, num_workers=num_workers)
    benchmark_kvs(num_splits, input_files)
