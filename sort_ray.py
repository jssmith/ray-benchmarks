import sys
#import ray
import random

from utils import Timer, chunks

def usage():
    print "Usage: raysort num_workers num_splits inputfile [inputfile ...]"

# @ray.remote
# def gen_text(start_id, num_keys, seed):
#     np.random.seed(23435)
#     return [teragen.get_row(i) for i in range(start_id, start_id + num_keys)]
def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

#@ray.remote
def load_files(input_files):
    res = [line for input_file in input_files for line in read_input(input_file)]
    print 'loaded {} : {}'.format(str(input_files), len(res))
    return res

#@ray.remote
def sample_input(input, num_samples, random_seed):
    random.seed(random_seed)
    res = []
    for _ in range(num_samples):
        res.append(input[random.randint(0, len(input) - 1)])
    return res

def sort_split(input, split_points):
    si = sorted(input)
    last_split_point = 0
    split_results = []
    for split_point in split_points:
        next_split_point = next(i for i in xrange(last_split_point, len(si)) if si[i] > split_point)
        # print "next split point at {} on total length {}".format(next_split_point, len(si))
        split_results.append(si[last_split_point:next_split_point])
        last_split_point = next_split_point
    split_results.append(si[last_split_point:])
    # print "number of split points is {}".format(len(split_points))

    # for res in split_results:
    #     print "split from '{}' to '{}'".format(res[0][:10], res[-1][:10])

    return split_results

def transpose(listoflists):
    return map(list, zip(*listoflists))

def merge_sorted(input_splits):
    # print "merge {} splits".format(len(input_splits))
    # for res in input_splits:
    #     print "split from '{}' to '{}'".format(res[0][:10], res[-1][:10])

    # todo merge sort
    res = sorted([line for split in input_splits for line in split])
    # print "have range '{}'' to '{}'".format(res[0][:10], res[-1][:10])
    return res

def benchmark_sort(num_workers, num_splits, input_files):
    t = Timer("sort")
    file_chunks = chunks(input_files, num_splits)
    # print "file chunks", list(file_chunks)
    # assume uniform file sizes
    inputs = [load_files(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    # sample each input
    samples = map(lambda (input, index): sample_input(input, 10, index), zip(inputs, range(len(inputs))))

    # flatten samples
    samples_sorted = sorted([sample for chunk_samples in samples for sample in chunk_samples])
    # print samples_sorted

    num_samples = len(samples_sorted)
    samples_per_split = float(num_samples) / num_splits
    split_points = []
    split_point = samples_per_split
    while split_point < num_samples:
        split_points.append(samples_sorted[int(split_point)])
        print "split point at '{}...'".format(split_points[-1][:10])
        split_point += samples_per_split

    ss = [sort_split(input, split_points) for input in inputs]
    res = map(merge_sorted, transpose(ss))
    # TODO be sure to get
    t.finish()



if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    benchmark_sort(num_workers, num_splits, input_files)
