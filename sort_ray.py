import sys
import ray
import random

from utils import Timer, chunks, transpose

def usage():
    print "Usage: sort_ray num_workers num_splits inputfile [inputfile ...]"

def read_input(input_file):
    with open(input_file) as f:
        return map(lambda x: x.rstrip(), f.readlines())

@ray.remote
def load_files(input_files):
    t = Timer('load ' + ', '.join(input_files))
    res = [line for input_file in input_files for line in read_input(input_file)]
    print 'loaded {} : {}'.format(str(input_files), len(res))
    t.finish(len(res))
    return res

@ray.remote
def sample_input(input, num_samples, random_seed):
    t = Timer('sample')
    random.seed(random_seed)
    res = []
    for _ in range(num_samples):
        res.append(input[random.randint(0, len(input) - 1)])
    t.finish()
    return res

@ray.remote
def sort_split(input, split_points):
    t = Timer('sort_split')
    si = sorted(input)
    last_split_point = 0
    split_results = []
    for split_point in split_points:
        next_split_point = next(i for i in xrange(last_split_point, len(si)) if si[i] > split_point)
        # print "next split point at {} on total length {}".format(next_split_point, len(si))
        split_results.append(ray.put(si[last_split_point:next_split_point]))
        last_split_point = next_split_point
    split_results.append(ray.put(si[last_split_point:]))
    # print "number of split points is {}".format(len(split_points))

    # for res in split_results:
    #     print "split from '{}' to '{}'".format(res[0][:10], res[-1][:10])
    t.finish()
    return split_results

@ray.remote
def merge_sorted(input_splits):
    t = Timer('merge')
    # print "merge {} splits".format(len(input_splits))
    # for res in input_splits:
    #     print "split from '{}' to '{}'".format(res[0][:10], res[-1][:10])

    # todo merge sort since inputs already sorted
    res = sorted([line for split in input_splits for line in ray.get(split)])
    # print "have range '{}'' to '{}'".format(res[0][:10], res[-1][:10])
    t.finish()
    return res

def benchmark_sort(num_splits, input_files):
    t = Timer("RAY_BENCHMARK_SORT")
    file_chunks = chunks(input_files, num_splits)
    # print "file chunks", list(file_chunks)
    # assume uniform file sizes
    inputs = [load_files.remote(chunk_files) for chunk_files in chunks(input_files, num_splits)]

    # sample each input
    samples = map(lambda (input, index): sample_input.remote(input, 10, index), zip(inputs, range(len(inputs))))

    # flatten samples
    samples_sorted = sorted([sample for chunk_samples in samples for sample in ray.get(chunk_samples)])
    # compute sample splits
    num_samples = len(samples_sorted)
    samples_per_split = float(num_samples) / num_splits
    split_points = []
    split_point = samples_per_split
    while split_point < num_samples:
        split_points.append(samples_sorted[int(split_point)])
        print "split point at '{}...'".format(split_points[-1][:10])
        split_point += samples_per_split

    ss = [sort_split.remote(input, split_points) for input in inputs]
    res = map(merge_sorted.remote, transpose([ray.get(s) for s in ss]))
    # TODO be sure to get
    [ray.get(r) for r in res]
    t.finish()



if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    input_files = sys.argv[3:]
    ray.init(start_ray_local=True, num_workers=num_workers)
    for _ in range(1):
        benchmark_sort(num_splits, input_files)
