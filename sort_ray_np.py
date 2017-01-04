import sys
import random
import numpy as np

import ray

from utils import Timer, chunks, transpose

def usage():
    print "Usage: sort_ray_np num_workers num_splits inputfile [inputfile ...]"

@ray.remote
def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

@ray.remote
def sample_input(input, num_samples, random_seed):
    t = Timer('sample')
    random.seed(random_seed)
    res = np.empty(num_samples, dtype=input.dtype)
    for i in range(num_samples):
        res[i] = input[random.randint(0, len(input) - 1)]
    t.finish()
    return res

@ray.remote
def sort_split(input, split_points):
    t = Timer('sort_split')
    si = np.sort(input)
    last_split_point = 0
    split_results = []
    for split_point in split_points:
        next_split_point = next(i for i in xrange(last_split_point, len(si)) if si[i] > split_point)
        split_results.append(ray.put(si[last_split_point:next_split_point]))
        last_split_point = next_split_point
    split_results.append(ray.put(si[last_split_point:]))
    t.finish()
    return split_results

@ray.remote
def merge_sorted(input_splits):
    t = Timer('merge')
    # todo - maybe merge sort since inputs already sorted
    res = np.sort(np.concatenate([ray.get(input) for input in input_splits]))
    t.finish()
    return res

def benchmark_sort(num_splits, input_files):
    t = Timer("RAY_BENCHMARK_SORT")
    file_chunks = chunks(input_files, num_splits)
    # print "file chunks", list(file_chunks)
    # assume uniform file sizes
    inputs = [load_files.remote(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    # sample each input
    # todo - number of samples proprtional to number of records
    samples = map(lambda (input, index): sample_input.remote(input, 10, index), zip(inputs, range(len(inputs))))

    # flatten samples
    samples_sorted = np.sort(np.concatenate([ray.get(sample) for sample in samples]))
    # compute sample splits
    num_samples = len(samples_sorted)
    samples_per_split = float(num_samples) / num_splits
    split_points = []
    split_point = samples_per_split
    while split_point < num_samples:
        split_points.append(samples_sorted[int(split_point)])
        print "split point at '{}...'".format(split_points[-1][:10])
        split_point += samples_per_split

    ss = [sort_split.remote(input, split_points) for input in inputs]
    res = map(merge_sorted.remote, transpose([ray.get(s) for s in ss]))
    # TODO be sure to get
    [ray.get(r) for r in res]
    t.finish()



if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    ray.init(start_ray_local=True, num_workers=num_workers)
    for _ in range(3):
        benchmark_sort(num_splits, input_files)
