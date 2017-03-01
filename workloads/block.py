import json
import ray
import raybench

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

    with eventstats.BenchmarkLogSpan("submit"):
        ray.get([g.remote(i) for i in range(num_tasks)])


if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    num_tasks = int(2 ** (bench_env.benchmark_iteration / 4.))
    print("Attempting to execute {0} blocking tasks".format(num_tasks))

    test_blocking_tasks(num_tasks)

    print "BENCHMARK_STATS:", json.dumps({
        "config": { "num_tasks" : num_tasks },
        "events" : eventstats.log_span_events() })
