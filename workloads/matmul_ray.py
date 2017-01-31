import sys
import os
import ray
import numpy as np

from math import sqrt, ceil

from raybench import benchmark_init, benchmark_measure
from raybench.utils import Timer, init_np_env


@ray.remote
def mat_gen_block(i, j, dim):
    init_np_env()
    np.random.seed(hash((i,j)) % 4294967296)
    print "generating {}x{} matrix".format(dim, dim)
    m = np.random.rand(dim, dim)
    return m

def mat_gen(block_size, dim_blocks):
    with benchmark_init():
        matrix_blocks = [[mat_gen_block.remote(bi, bj, block_size) for bj in range(dim_blocks)] for bi in range(dim_blocks)]
        for job_list in matrix_blocks:
            for job in job_list:
                ray.wait([job])
        return matrix_blocks

@ray.remote
def mult_dim(a, b):
    init_np_env()
    if len(a) != len(b):
        print "lengths must be equal"
        return None
    return reduce(np.add, [np.dot(ray.get(a[i]), ray.get(b[i])) for i in range(len(a))])

def benchmark_matmul(blocks, dim_blocks):
    init_np_env()

    def mult_block(i, j):
        a = [blocks[i][k] for k in range(dim_blocks)]
        b = [blocks[k][j] for k in range(dim_blocks)]
        return mult_dim.remote(a, b)

    with benchmark_measure():
        res = [[mult_block(i, j) for j in range(dim_blocks)] for i in range(dim_blocks)]
        [ray.wait([res[i][j]]) for i in range(dim_blocks) for j in range (dim_blocks)]

if __name__ == '__main__':
    if "RAY_NUM_WORKERS" in os.environ:
        num_workers = int(os.environ["RAY_NUM_WORKERS"])
    else:
        num_workers = 4
    num_splits = int(ceil(sqrt(num_workers)))
    if 'RAY_REDIS_ADDRESS' in os.environ:
        address_info = ray.init(redis_address=os.environ['RAY_REDIS_ADDRESS'])
    else:
        print "No Redis address - starting locally"
        address_info = ray.init(num_workers=num_workers)

    matrix = mat_gen(2000, num_splits)
    benchmark_matmul(matrix, num_splits)
    ray.flush_log()
