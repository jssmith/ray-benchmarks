import sys
import ray
from random import shuffle
from utils import Timer

def usage():
    print "Usage: np_kvs_ray num_workers num_splits num_iterations"

def get_all(objects):
    t = Timer('query')
    n = len(objects)
    dist_sum = 0
    for i in range(n):
        dist_sum += ray.get(objects[i]) - i
    print dist_sum
    t.finish(n)

def benchmark(num_splits, n):
    objects = [ray.put(i) for i in range(n)]
    shuffle(objects)
    # compute the average distance on shuffled objects
    get_all(objects)
    shuffle(objects)
    get_all(objects)
    # for i in range(n):
    #     print ray.get(objects[i]) - i

if __name__ == '__main__':
    if len(sys.argv) != 4:
        usage()
        sys.exit(1)
    num_workers = int(sys.argv[1])
    num_splits = int(sys.argv[2])
    num_iterations = int(sys.argv[3])
    ray.init(start_ray_local=True, num_workers=num_workers)
    benchmark(num_splits, num_iterations)
