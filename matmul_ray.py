import sys
import os
import ray
import numpy as np
from utils import Timer, init_np_env
from sweep import sweep_iterations
from matgen import filename_format_str
import event_stats

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
    with event_stats.benchmark_init():
        blocks = [[load_block.remote(input_prefix, i, j) for j in range(dim_blocks)] for i in range(dim_blocks)]
        [ray.wait([blocks[i][j]]) for i in range(dim_blocks) for j in range (dim_blocks)]

    with event_stats.benchmark_measure():
        res = [[None for i in range(dim_blocks)] for j in range(dim_blocks)]
        for i in range(dim_blocks):
            for j in range(dim_blocks):
                a = [blocks[i][k] for k in range(dim_blocks)]
                b = [blocks[k][j] for k in range(dim_blocks)]
                res[i][j] = mult_dim.remote(a, b)

        [ray.wait([res[i][j]]) for i in range(dim_blocks) for j in range (dim_blocks)]

if __name__ == '__main__':
    if len(sys.argv) != 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_prefix = sys.argv[3]
    if 'REDIS_ADDRESS' in os.environ:
        redis_address = os.environ['REDIS_ADDRESS']
        print "using Ray with Redis at {}".format(redis_address)
        address_info = ray.init(redis_address=redis_address)
    else:
        print "starting Ray with {} workers".format(num_workers)
        address_info = ray.init(start_ray_local=True, num_workers=num_workers)
    for _ in range(sweep_iterations):
        benchmark_matmul(input_prefix, num_splits)
    ray.flush_log()
    config_info = {
        'benchmark_name' : 'matmul',
        'benchmark_implementation' : 'ray',
        'benchmark_iterations' : sweep_iterations,
        'num_workers' : num_workers,
        'num_splits' : num_splits,
        'input_file_base' : input_prefix,
        'num_inputs' : num_splits * num_splits
    }
    event_stats.print_stats_summary(config_info, address_info['redis_address'])
