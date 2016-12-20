#import ray
import sys
from utils import transpose
import hashlib

from collections import defaultdict

from random import shuffle

from utils import Timer

key_len = 10

def usage():
    print "Usage: kvs_ray num_workers num_splits inputfile [inputfile ...]"

def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

#@ray.remote
def load_files(input_files):
    res = [line for input_file in input_files for line in read_input(input_file)]
    print 'loaded {} : {}'.format(str(input_files), len(res))
    return res

#@ray.remote
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

#@ray.remote
def merge_hashed(input_splits):
    # for i in input_splits:
    #     print len(i)
    res = dict([(line[:key_len], line) for input in input_splits for line in input])
    print "have dict of size", len(res)
    return res

def lookup(key, kvs_blocks):
    hash_value = hash(key)
    num_splits = len(kvs_blocks)
    return kvs_blocks[hash_value % num_splits][key]

def setup(input_files):
    t_load = Timer('load')
    inputs = load_files(input_files)
    print 'number of splits is', num_splits
    hs = [hash_split(inputs, num_splits)]
    print 'ns', len(hs[0])
    res = map(merge_hashed, transpose([s for s in hs]))

    # finish setting up kvs
    #kvs_blocks = [ray.get(r) for r in res]
    kvs_blocks = [r for r in res]
    t_load.finish()

    # query the kvs
    query_keys = [line[:key_len] for line in read_input(input_files[0])]
    shuffle(query_keys)
    t_lookup = Timer('lookup')
    ct = 0
    for key in query_keys:
        lookup(key, kvs_blocks)
        ct += 1
    print "lookup count is", ct
    t_lookup.finish()


# def read_keys():

def benchmark_kvs(num_workers, num_splits, input_files):
    blocks = setup(input_files)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    benchmark_kvs(num_workers, num_splits, input_files)
