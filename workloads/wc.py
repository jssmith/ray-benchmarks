import os
import sys
import re
import ray
import numpy as np
import raybench

from collections import defaultdict


@ray.remote
def wc_gen(source_text, start_index, end_index):
    print "generating {} words on range {} - {}".format(end_index - start_index, start_index, end_index)
    np.random.seed((start_index * 71741 + end_index) % 4294967296)
    words = []
    for line in source_text.split("\n"):
        line = re.sub(r"[^a-zA-Z']", " ", line)
        line = re.sub(r"  +", " ", line)
        line = line.strip()
        words += [word for word in line.split(' ')]
    return " ".join([words[np.random.randint(len(words))] for _ in range(start_index, end_index)])


@ray.remote
def wc(input_splits):
    word_counters = defaultdict(lambda: 0)
    for input_split in input_splits:
        text = ray.get(input_split)
        text = re.sub(r"[^a-zA-Z]", " ", text)
        text = re.sub(r"  +", " ", text)
        for word in filter(lambda x: len(x) > 0, map(lambda x: x.lower(), text.split(' '))):
            word_counters[word] += 1
    return word_counters


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

def init_wc(num_splits, words_per_split):
    with open('workloads/lear.txt', 'r') as f:
        source_text = f.read()
    with raybench.init():
        gen_jobs = list([wc_gen.remote(source_text, i, i + words_per_split) for i in range(0, num_splits * words_per_split, words_per_split)])
        ray.wait(gen_jobs, num_returns=num_splits)
        return gen_jobs


def do_wc(input_splits):
    with raybench.measure():
        results = [wc.remote(input_split) for input_split in input_splits]
        print "number of results is {}".format(len(results))
        res = tree_reduce_remote.remote(dict_merge, results)

        ray.wait([res])

        # find most common word
        most_popular_word = None
        most_popular_ct = 0
        for word, ct in ray.get(res).items():
            if ct > most_popular_ct:
                most_popular_word = word
                most_popular_ct = ct
        print "most popular word is '{}' with count {}".format(most_popular_word, most_popular_ct)

def chunks(l, n):
    chunk_size = (len(l) - 1) / n + 1
    for i in xrange(0, len(l), chunk_size):
        yield l[i:i + chunk_size]

if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()
    ray.register_class(type(dict_merge.remote), pickle=True)

    num_splits = bench_env.num_workers

    input_splits = init_wc(num_splits, 100000)
    print "number of splits", len(input_splits)
    do_wc(input_splits)

    ray.flush_log()
