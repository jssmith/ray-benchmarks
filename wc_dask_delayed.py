import sys
import re

import wc as wclib

import dask
from dask import delayed
import dask.multiprocessing
import multiprocessing.pool

from collections import defaultdict

from utils import Timer, chunks

def usage():
    print "Usage: wc_dask_delayed.py num_workers num_splits inputfile [inputfile ...]"

def tree_reduce(fn, data):
    if len(data) == 1:
        return data[0]
    elif len(data) == 2:
        return delayed(fn)(data[0], data[1])
    else:
        mid = len(data)/2
        x = tree_reduce(fn, data[:mid])
        y = tree_reduce(fn, data[mid:])
        return delayed(fn)(x, y)

def dict_merge(x, y):
    print "merge dict of length {} and of length {}".format(len(x), len(y))
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

def do_wc(um_splits, input_files):
    t = Timer("RAY_BENCHMARK_WC")
    results = [delayed(wclib.wc)(inputs) for inputs in chunks(input_files, num_splits)]
    print "number of results is {}".format(len(results))
    #res = reduce(dict_merge.remote, results)
    res = tree_reduce(dict_merge, results)

    res_computed = res.compute()

    # find most common word
    most_popular_word = None
    most_popular_ct = 0
    for word, ct in res_computed.items():
        if ct > most_popular_ct:
            most_popular_word = word
            most_popular_ct = ct
    print "most popular word is '{}' with count {}".format(most_popular_word, most_popular_ct)
    t.finish()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    dask.set_options(get=dask.multiprocessing.get)
    dask.set_options(pool=multiprocessing.pool.Pool(num_workers))
    do_wc(num_splits, input_files)
