import json
import ray
import raybench

import raybench.eventstats as eventstats

if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    n = int(10 ** bench_env.benchmark_iteration)

    with eventstats.BenchmarkLogSpan("ones_array"):
        x = n * [1]
        ray.put(x)

    print "BENCHMARK_STATS:", json.dumps({
        "config": { "scale" : n },
        "events" : eventstats.log_span_events() })
