import sys
import re

from distributed import Client
import wc as wclib

from collections import defaultdict

from utils import Timer, chunks

def usage():
    print "Usage: wc_dask_distributed.py num_workers num_splits inputfile [inputfile ...]"

def tree_reduce(client, fn, data):
    if len(data) == 1:
        return data[0]
    elif len(data) == 2:
        return client.submit(fn, data[0], data[1])
    else:
        mid = len(data)/2
        x = tree_reduce(client, fn, data[:mid])
        y = tree_reduce(client, fn, data[mid:])
        return client.submit(fn, x, y)

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

def do_wc(client, num_splits, input_files):
    t = Timer("RAY_BENCHMARK_WC")
    results = list(client.map(wclib.wc, chunks(input_files, num_splits)))
    res = tree_reduce(client, dict_merge, results)
    res_computed = res.result()

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
    client = Client()
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    do_wc(client, num_splits, input_files)