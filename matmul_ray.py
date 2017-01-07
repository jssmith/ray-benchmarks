import sys
import os
import ray
import numpy as np
from utils import Timer, init_np_env
from matgen import filename_format_str

def usage():
    print "Usage: matmul_ray.py num_workers num_splits input_prefix"

@ray.remote
def load_block(input_prefix, i, j):
    filename = filename_format_str.format(input_prefix, i, j)
    return np.load(filename)

@ray.remote
def mult_dim(a, b):
    init_np_env()
    if len(a) != len(b):
        print "lengths must be equal"
        return None
    return reduce(np.add, [np.dot(ray.get(a[i]), ray.get(b[i])) for i in range(len(a))])

def benchmark_matmul(input_prefix, dim_blocks):
    init_np_env()
    blocks = [[load_block.remote(input_prefix, i, j) for j in range(dim_blocks)] for i in range(dim_blocks)]
    [ray.wait([blocks[i][j]]) for i in range(dim_blocks) for j in range (dim_blocks)]

    t = Timer("RAY_BENCHMARK_MATMUL")
    res = [[None for i in range(dim_blocks)] for j in range(dim_blocks)]
    for i in range(dim_blocks):
        for j in range(dim_blocks):
            a = [blocks[i][k] for k in range(dim_blocks)]
            b = [blocks[k][j] for k in range(dim_blocks)]
            res[i][j] = mult_dim.remote(a, b)

    [ray.wait([res[i][j]]) for i in range(dim_blocks) for j in range (dim_blocks)]    
    t.finish()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_prefix = sys.argv[3]
    print "starting Ray with {} workers".format(num_workers)
    ray.init(start_ray_local=True, num_workers=num_workers)
    benchmark_matmul(input_prefix, num_splits)