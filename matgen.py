import ray
import os
import sys
import random
import time
import numpy as np

from utils import Timer, init_np_env

filename_format_str = "{}_{:03d}_{:03d}.npy"

@ray.remote
def mat_gen(filename, dim):
    init_np_env()
    np.random.seed(hash(filename) % 4294967296)
    m = np.random.rand(dim, dim)
    np.save(filename, m, allow_pickle=False)
    print "generating {}x{} matrix {}".format(dim, dim, filename)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: matgen.py num_workers dim_size dim_blocks file_prefix"
        sys.exit(1)
    num_workers = int(sys.argv[1])
    dim_size = int(sys.argv[2])
    dim_blocks = int(sys.argv[3])
    file_prefix = sys.argv[4]
    ray.init(start_ray_local=True, num_workers=num_workers)
    if dim_size % dim_blocks != 0:
        print "dimension size must be multiple of dimension blocks"
        sys.exit(1)
    block_size = dim_size / dim_blocks
    jobs = []
    for bi in range(dim_blocks):
        for bj in range(dim_blocks):
            filename = filename_format_str.format(file_prefix, bi, bj)
            jobs.append(mat_gen.remote(filename, block_size))
    for job in jobs:
        ray.wait([job])
