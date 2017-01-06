import ray
import sys
import random
# import time
import numpy as np
import re

from utils import Timer

@ray.remote
def wc_gen(filename, start_index, end_index):
    print "generating {} words to {}".format(end_index - start_index, filename)
    np.random.seed((start_index * 71741 + end_index) % 4294967296)
    with open('lear.txt', 'r') as lear:
        words = []
        for line in lear.readlines():
            line = re.sub(r"[^a-zA-Z']", " ", line)
            line = re.sub(r"  +", " ", line)
            line = line.strip()
            words += [word for word in line.split(' ')]
    with open(filename, 'w') as f:
        ct = 0
        for _ in range(start_index, end_index):
            f.write(words[np.random.randint(len(words))])
            ct += 1
            if ct % 10 == 0:
                f.write('\n')
            else:
                f.write(' ')


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: wcgen.py num_workers num_splits num_words file_prefix"
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    num_words = int(sys.argv[3])
    file_prefix = sys.argv[4]
    filename_format_str = "{}_{:03d}"
    ray.init(start_ray_local=True, num_workers=num_workers)
    end_index = 0
    index_delta = float(num_words) / num_splits
    jobs = []
    for i in range(num_splits):
        filename = filename_format_str.format(file_prefix, i)
        start_index = end_index
        if i < num_splits - 1:
            end_index = start_index + index_delta
        else:
            end_index = num_words
        end_index_int = int(end_index)
        start_index_int = int(start_index)
        jobs.append(wc_gen.remote(filename, start_index_int, end_index_int))
    for job in jobs:
        ray.get(job)
