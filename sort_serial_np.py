import sys
import numpy as np

from utils import Timer
from sweep import sweep_iterations

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
    t = Timer("RAY_BENCHMARK_SERIALSORT")

    t_load = Timer("load")
    lines = load_files(input_files)
    t_load.finish(len(lines))
    print_sample(lines, 5)

    t_sort = Timer("sort")
    ls = np.sort(lines)
    t_sort.finish(len(lines))
    print_sample(ls, 5)

    t.finish()



if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    input_files = sys.argv[1:]
    for _ in range(sweep_iterations):
        benchmark_sort(input_files)
