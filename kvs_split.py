import sys
import hashlib
import random
import numpy as np

from collections import defaultdict
from random import shuffle
from utils import Timer, chunks, transpose

key_len = 10
queries_per_split = 100

def usage():
    print "Usage: kvs_split.py num_splits inputfile [inputfile ...]"

def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

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

def merge_hashed(input_splits):
    res = dict([(line[:key_len], line) for input in input_splits for line in input])
    print "have dict of size", len(res)
    return res

def sample_input(input, frac):
    num_samples = int(frac * len(input))
    random.seed(hash(input[0]))
    shuffle(input)
    samples = np.empty(num_samples, '|S10')
    for i in range(num_samples):
        samples[i] = input[i][:10]
    return samples

def _lookup(key, kvs_block):
    # t = Timer('lookup')
    ret = kvs_block[key]
    # t.finish()
    return ret

def lookup(key, kvs_blocks):
    hash_value = hash(key)
    num_splits = len(kvs_blocks)
    return _lookup(key, kvs_blocks[hash_value % num_splits])

def query(query_keys, kvs_blocks):
    t = Timer('query')
    ct = 0
    sumlen = 0
    t_loop = Timer('query-loop')
    for key in query_keys:
        sumlen += len(lookup(key, kvs_blocks))
        ct += 1
    t_loop.finish(queries_per_split)
    print "lookup count is", ct
    print "total length is", sumlen
    t.finish()
    return (ct, sumlen)

def benchmark_kvs(num_splits, input_files):
    t_load = Timer('load')
    inputs = [load_files(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    hs = [hash_split(input, num_splits) for input in inputs]
    res = map(merge_hashed, transpose([s for s in hs]))

    kvs_blocks = [r for r in res]

    input_samples = [sample_input(input, .01) for input in inputs]
    query_keys = np.concatenate([input_sample for input_sample in input_samples])

    t_load.finish()

    t_query = Timer('RAY_BENCHMARK_KVS')
    queries = [query(query_keys, kvs_blocks) for chunk_files in chunks(input_files, num_splits)]
    t_query.finish()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)
    num_splits = int(sys.argv[1])
    input_files = sys.argv[2:]
    benchmark_kvs(num_splits, input_files)
