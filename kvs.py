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
    print "Usage: kvs.py inputfile [inputfile ...]"

def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

def sample_input(input, frac):
    num_samples = int(frac * len(input))
    random.seed(hash(input[0]))
    shuffle(input)
    samples = np.empty(num_samples, '|S10')
    for i in range(num_samples):
        samples[i] = input[i][:10]
    return samples

def benchmark_kvs(input_files):
    t_load = Timer('load')
    inputs = load_files(input_files)


    m = {}
    for line in inputs:
        key = line[:key_len]
        m[key] = line

    input_samples = sample_input(inputs, .01)
    t_load.finish()

    t_query = Timer('RAY_BENCHMARK_KVS')
    t_query_rate = Timer('kvs queries')
    sumlen = 0
    ct = 0
    for key in input_samples:
        sumlen += len(m[key])
        ct += 1
    print "lookup count is", ct
    print "total length is", sumlen
    t_query.finish()
    t_query_rate.finish(ct)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    input_files = sys.argv[1:]
    benchmark_kvs(input_files)
