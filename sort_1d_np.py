import sys
import random
import numpy as np

from utils import Timer, chunks, transpose
from sweep import sweep_iterations

import event_stats

def usage():
    print "Usage: sort_1d num_splits inputfile [inputfile ...]"


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
    t = Timer('sort_split')
    si = np.sort(input)
    last_split_point = 0
    split_results = []
    for split_point in split_points:
        next_split_point = next(i for i in xrange(last_split_point, len(si)) if si[i] > split_point)
        split_results.append(si[last_split_point:next_split_point])
        last_split_point = next_split_point
    split_results.append(si[last_split_point:])
    t.finish()
    return split_results

def merge_sorted(input_splits):
    t = Timer('merge')
    # todo - maybe merge sort since inputs already sorted
    res = np.sort(np.concatenate(input_splits))
    t.finish()
    return res

def benchmark_sort(num_splits, input_files):
    with event_stats.benchmark_init_noray():
        file_chunks = chunks(input_files, num_splits)
        # print "file chunks", list(file_chunks)
        # assume uniform file sizes
        inputs = [load_files(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    with event_stats.benchmark_measure_noray():
        # sample each input
        # todo - number of samples proprtional to number of records
        samples = map(lambda (input, index): sample_input(input, 10, index), zip(inputs, range(len(inputs))))

        # flatten samples
        samples_sorted = np.sort(np.concatenate([sample for sample in samples]))
        # compute sample splits
        num_samples = len(samples_sorted)
        samples_per_split = float(num_samples) / num_splits
        split_points = []
        split_point = samples_per_split
        while split_point < num_samples:
            split_points.append(samples_sorted[int(split_point)])
            print "split point at '{}...'".format(split_points[-1][:10])
            split_point += samples_per_split

        ss = [sort_split(input, split_points) for input in inputs]
        res = map(merge_sorted, transpose([s for s in ss]))
        # TODO be sure to get


if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)
    num_splits = int(sys.argv[1])
    input_files = sys.argv[2:]
    for _ in range(sweep_iterations):
        benchmark_sort(num_splits, input_files)
    config_info = {
        'benchmark_name' : 'sort',
        'benchmark_implementation' : 'serial_raylike',
        'num_splits' : num_splits,
        'benchmark_iterations' : sweep_iterations,
        'input_file_base' : input_files[0],
        'num_inputs' : len(input_files)
    }
    event_stats.print_stats_summary_noray(config_info)