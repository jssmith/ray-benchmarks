import sys
import re

import ray
import wc as wclib

from collections import defaultdict

from utils import Timer, chunks

def usage():
    print "Usage: wc num_workers num_splits inputfile [inputfile ...]"

@ray.remote
def wc(input_files):
    return wclib.wc(input_files)

@ray.remote
def tree_reduce(fn, data):
    if len(data) == 1:
        return data[0]
    elif len(data) == 2:
        return fn(data[0], data[1])
    else:
        mid = len(data)/2
        return fn(ray.get(tree_reduce.remote(fn, data[:mid])), ray.get(tree_reduce.remote(fn, data[mid:])))


@ray.remote
def tree_reduce_remote(fn, data):
    if len(data) == 1:
        return ray.get(data[0])
    elif len(data) == 2:
        return ray.get(fn.remote(data[0], data[1]))
    else:
        mid = len(data)/2
        return ray.get(fn.remote(tree_reduce_remote.remote(fn, data[:mid]), tree_reduce_remote.remote(fn, data[mid:])))

@ray.remote
def tree_reduce_merge(data):
    if len(data) == 1:
        return data[0]
    elif len(data) == 2:
        return dict_merge.remote(data[0], data[1])
    else:
        mid = len(data)/2
        return dict_merge.remote(tree_reduce_merge.remote(data[:mid]), tree_reduce_merge.remote(data[mid:]))

# variable to test: how many to merge at once
@ray.remote
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

def do_wc(num_workers, num_splits, input_files):
    ray.register_class(type(dict_merge.remote), pickle=True)
    print "starting Ray with {} workers".format(num_workers)
    ray.init(start_ray_local=True, num_workers=num_workers)
    t = Timer("multi")
    results = [wc.remote(input_file) for input_file in chunks(input_files, num_splits)]
    print "number of results is {}".format(len(results))
    #res = reduce(dict_merge.remote, results)
    res = tree_reduce_remote.remote(dict_merge, results)

    # find most common word
    most_popular_word = None
    most_popular_ct = 0
    for word, ct in ray.get(res).items():
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
    do_wc(num_workers, num_splits, input_files)
