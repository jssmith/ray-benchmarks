import sys
import os
import numpy as np
from utils import Timer, init_np_env
from matgen import filename_format_str

from sweep import sweep_iterations
import event_stats

def usage():
    print "Usage: matmul.py num_splits input_prefix"

def load_block(input_prefix, i, j):
    filename = filename_format_str.format(input_prefix, i, j)
    return np.load(filename)

def benchmark_matmul(input_prefix, dim_blocks):
    init_np_env()
    with event_stats.benchmark_init_noray():
        a = np.concatenate([np.concatenate([load_block(input_prefix, i, j) for j in range(dim_blocks)], axis=1) for i in range(dim_blocks)])
        b = np.concatenate([np.concatenate([load_block(input_prefix, i, j) for j in range(dim_blocks)], axis=1) for i in range(dim_blocks)])

    with event_stats.benchmark_measure_noray():
        c = np.dot(a, b)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    num_splits = int(sys.argv[1])
    input_prefix = sys.argv[2]

    for _ in range(sweep_iterations):
        benchmark_matmul(input_prefix, num_splits)
    config_info = {
        'benchmark_name' : 'matmul',
        'benchmark_implementation' : '1-thread',
        'benchmark_iterations' : sweep_iterations,
        'input_file_base' : input_prefix,
        'num_inputs' : num_splits * num_splits
    }
    event_stats.print_stats_summary_noray(config_info)
