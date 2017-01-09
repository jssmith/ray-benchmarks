import sys
import random
import numpy as np

from distributed import Client, local_client
from distributed.client import wait

from utils import Timer, chunks, transpose
from sweep import sweep_iterations

import event_stats

def usage():
    print "Usage: sort_ray_np num_workers num_splits inputfile [inputfile ...]"

def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

def sample_input(input, num_samples, random_seed):
    t = Timer('sample')
    random.seed(random_seed)
    res = np.empty(num_samples, dtype=input.dtype)
    for i in range(num_samples):
        res[i] = input[random.randint(0, len(input) - 1)]
    t.finish()
    return res

def sort_split(input, split_points):
    with local_client() as lc:
        print "START SORT SPLIT"
        t = Timer('sort_split')
        si = np.sort(input)
        last_split_point = 0
        split_results = []
        for split_point in split_points:
            next_split_point = next(i for i in xrange(last_split_point, len(si)) if si[i] > split_point)
            split_results.append(lc.submit(lambda x: x, si[last_split_point:next_split_point]))
            last_split_point = next_split_point
        split_results.append(lc.submit(lambda x: x, si[last_split_point:]))
        t.finish()
        print "END SORT SPLIT"
    return split_results

def merge_sorted(input_splits):
    t = Timer('merge')
    res = np.sort(np.concatenate(input_splits))
    return res

def get_split_points(samples_sorted, num_splits):
    num_samples = len(samples_sorted)
    samples_per_split = float(num_samples) / num_splits
    split_points = []
    split_point = samples_per_split
    while split_point < num_samples:
        split_points.append(samples_sorted[int(split_point)])
        print "split point at '{}...'".format(split_points[-1][:10])
        split_point += samples_per_split
    return split_points

def input_split_indexes(input_sorted, split_points):
    last_split_point = 0
    split_results = []
    for split_point in split_points:
        next_split_point = next(i for i in xrange(last_split_point, len(input_sorted)) if input_sorted[i] > split_point)
        split_results.append((last_split_point, next_split_point))
        last_split_point = next_split_point
    split_results.append((last_split_point,len(input_sorted)))
    print "splits at ", split_results
    return split_results

def extract_range(input_sorted, r, j):
    s = r[j][0]
    e = r[j][1]
    return input_sorted[s:e]

def benchmark_sort(client, num_splits, input_files):
    with event_stats.benchmark_init_noray():
        inputs = list(client.map(load_files, chunks(input_files, num_splits)))
        wait(inputs)

    with event_stats.benchmark_measure_noray():
        # sample each input
        # todo - number of samples proprtional to number of records
        samples = client.map(lambda (input, index): sample_input(input, 10, index), zip(inputs, range(len(inputs))))

        # flatten samples
        samples_sorted = client.submit(lambda x: np.sort(np.concatenate(x)), samples)

        # compute sample splits
        split_points = client.submit(get_split_points, samples_sorted, num_splits)

        inputs_sorted = client.map(np.sort, inputs)
        isi = [client.submit(input_split_indexes, input_sorted, split_points) for input_sorted in inputs_sorted]
        sorted_blocks = [[client.submit(extract_range, inputs_sorted[i], isi[i], j) for j in range(num_splits)] for i in range(num_splits)]

        res = [client.submit(merge_sorted, b) for b in transpose(sorted_blocks)]
        wait(res)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    client = Client()
    for _ in range(sweep_iterations):
        benchmark_sort(client, num_splits, input_files)
    config_info = {
        'benchmark_name' : 'sort',
        'benchmark_implementation' : 'dask_distributed',
        'num_workers' : num_workers,
        'num_splits' : num_splits,
        'benchmark_iterations' : sweep_iterations,
        'input_file_base' : input_files[0],
        'num_inputs' : len(input_files)
    }
    event_stats.print_stats_summary_noray(config_info)