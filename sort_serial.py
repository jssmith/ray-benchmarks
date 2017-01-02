import sys

from utils import Timer

def usage():
    print "Usage: sort_simple.py inputfile [inputfile ...]"

def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

def load_files(input_files):
    res = [line for input_file in input_files for line in read_input(input_file)]
    print 'loaded {} : {}'.format(str(input_files), len(res))
    return res


def benchmark_sort(input_files):
    t = Timer("RAY_BENCHMARK_SERIALSORT")

    t_load = Timer("load")
    lines = load_files(input_files)
    t_load.finish(len(lines))

    t_sort = Timer("sort")
    ls = sorted(lines)
    t_sort.finish()

    t.finish()



if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    input_files = sys.argv[1:]
    for _ in range(5):
        benchmark_sort(input_files)
