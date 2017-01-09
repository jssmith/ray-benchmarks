import sys
import numpy as np

from utils import Timer
from sweep import sweep_iterations

import event_stats

def usage():
    print "Usage: sort_serial_np.py inputfile [inputfile ...]"

def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

def load_files(input_files):
    res = np.concatenate([np.load(input_file) for input_file in input_files])
    print 'loaded {} : {}'.format(str(input_files), np.shape(res)[0])
    return res

def print_sample(lines, n):
    for i in range(n):
        print lines[i]

def benchmark_sort(input_files):
    with event_stats.benchmark_init_noray():
        t_load = Timer("load")
        lines = load_files(input_files)
        t_load.finish(len(lines))
        # print_sample(lines, 5)

    with event_stats.benchmark_measure_noray():
        t_sort = Timer("sort")
        ls = np.sort(lines)
        t_sort.finish(len(lines))
        # print_sample(ls, 5)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    input_files = sys.argv[1:]
    for _ in range(sweep_iterations):
        benchmark_sort(input_files)
    config_info = {
        'benchmark_name' : 'sort',
        'benchmark_implementation' : '1-thread',
        'benchmark_iterations' : sweep_iterations,
        'input_file_base' : input_files[0],
        'num_inputs' : len(input_files)
    }
    event_stats.print_stats_summary_noray(config_info)