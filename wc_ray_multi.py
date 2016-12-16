import sys
import re

import ray
import wc

from collections import defaultdict

def usage():
    print "Usage: wc inputfile [inputfile ...]"

@ray.remote
def wc(input_file):
    return wc.wc(input_file)

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

def do_wc(input_files):
    ray.register_class(type(dict_merge.remote), pickle=True)
    num_workers = min(4, len(input_files))
    print "starting Ray with {} workers".format(num_workers)
    ray.init(start_ray_local=True, num_workers=num_workers)
    results = [wc.remote(input_file) for input_file in input_files]
    print "number of results is {}".format(len(results))
    print results
    res = reduce(dict_merge.remote, results)
    #res = tree_reduce_remote.remote(dict_merge, results)

    # print ray.get(res)
    # find most common word
    most_popular_word = None
    most_popular_ct = 0
    for word, ct in ray.get(res).items():
        if ct > most_popular_ct:
            most_popular_word = word
            most_popular_ct = ct
    print "most popular word is '{}' with count {}".format(most_popular_word, most_popular_ct)
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    input_files = sys.argv[1:]
    do_wc(input_files)
