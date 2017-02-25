import json
import ray
import raybench
import raybench.event_stats as eventstats
import time

@ray.remote
def donothing():
    pass

def benchmark_launchtasks(num_tasks):
    with eventstats.BenchmarkLogSpan("total"):
        with eventstats.BenchmarkLogSpan("submit"):
            l = [donothing.remote() for _ in range(num_tasks)]
        ray.wait(l, num_returns=len(l))

if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    num_tasks = 10000
    benchmark_launchtasks(num_tasks)
    print "BENCHMARK_STATS:", json.dumps({
        "config": { "num_tasks" : num_tasks },
        "events" : eventstats.log_span_events() })
