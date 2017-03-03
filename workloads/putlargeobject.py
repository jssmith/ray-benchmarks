import json
import ray
import raybench


import raybench.eventstats as eventstats

def dotest(n):
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

    dotest(n)

    print "BENCHMARK_STATS:", json.dumps({
        "config": { "scale" : n },
        "events" : eventstats.log_span_events() })
