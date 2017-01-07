import sys
import os
import numpy as np
from utils import Timer, init_np_env
from matgen import filename_format_str

def usage():
    print "Usage: matmul.py num_splits input_prefix"

def load_block(input_prefix, i, j):
    filename = filename_format_str.format(input_prefix, i, j)
    return np.load(filename)

def benchmark_matmul(input_prefix, dim_blocks):
    init_np_env()
    a = np.concatenate([np.concatenate([load_block(input_prefix, i, j) for j in range(dim_blocks)], axis=1) for i in range(dim_blocks)])
    b = np.concatenate([np.concatenate([load_block(input_prefix, i, j) for j in range(dim_blocks)], axis=1) for i in range(dim_blocks)])
    t = Timer("RAY_BENCHMARK_MATMUL")
    c = np.dot(a, b)
    t.finish()
    return c

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    num_splits = int(sys.argv[1])
    input_prefix = sys.argv[2]
    
    benchmark_matmul(input_prefix, num_splits)