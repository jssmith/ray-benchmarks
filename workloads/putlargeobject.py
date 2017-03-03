import json
import ray
import raybench

import numpy as np

import raybench.eventstats as eventstats



def test_blocking_tasks(num_tasks):
    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another remote
        # task.
        object_ids = [f.remote(i, j) for j in range(10)]
        return ray.get(object_ids)

def dotest(n):
    with eventstats.BenchmarkLogSpan("np.ones"):
        x = np.ones(n)
        ray.put(x)
    with eventstats.BenchmarkLogSpan("ones_array"):
        x = n * [1]
        ray.put(x)
    with eventstats.BenchmarkLogSpan("randints"):
        list(np.random.randint(0, 100, size=n))
        ray.put(x)
    with eventstats.BenchmarkLogSpan("string"):
        x = n * "a"
        ray.put(x)
    with eventstats.BenchmarkLogSpan("array_of_pairs"):
        x = {i:i for i in range(n)}
        ray.put(x)
    with eventstats.BenchmarkLogSpan("ntuple"):
        x = n * (1,)
        ray.put(x)


if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    n = int(10 ** bench_env.benchmark_iteration)
    print("Attempting to execute with objects of size {0}".format(n))

    dotest(n)

    print "BENCHMARK_STATS:", json.dumps({
        "config": { "scale" : n },
        "events" : eventstats.log_span_events() })
