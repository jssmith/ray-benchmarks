
import sys
import random
import time
import numpy as np

from utils import Timer

def get_row(id):
    parts = []
    parts.append(''.join([chr(x) for x in np.random.randint(32,95, size=10)]))
    parts.append('{:012x}'.format(id))
    for i in range (7):
        parts.append(''.join([chr(32 + (id + i) % 95) for _ in range(10)]))
    parts.append(''.join([chr(32 + (id + 7) %95) for _ in range(8)]))
    return ''.join(parts)
    return 


def test_gen():
    """Timing tests for random file generation"""
    np.random.seed(23435)

    t1 = Timer('setup')
    lines = list([get_row(i) for i in range(40000)])
    t1.finish()

    t2 = Timer('sort')
    sl = sorted(lines)
    t2.finish()

    for line in sl[:10]:
        print line

def tera_gen(filename, start_index, end_index):
    np.random.seed((start_index * 71741 + end_index) % 4294967296)
    with open(filename, 'w') as f:
        for i in range(start_index, end_index):
            f.write(get_row(i))
            f.write('\n')

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage: teragen.py num_records num_splits file_prefix"
        sys.exit(1)
    num_records = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    file_prefix = sys.argv[3]
    end_index = 0
    index_delta = float(num_records) / num_splits
    for i in range(num_splits):
        filename = "{}_{:03d}".format(file_prefix, i)
        start_index = end_index
        if i < num_splits - 1:
            end_index = start_index + index_delta
        else:
            end_index = num_records
        end_index_int = int(end_index)
        start_index_int = int(start_index)
        print "generating {} records to {}".format(end_index_int - start_index_int, filename)
        tera_gen(filename, start_index_int, end_index_int)