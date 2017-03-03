import json
import ray
import raybench

import numpy as np

import raybench.eventstats as eventstats

if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    n = int(10 ** bench_env.benchmark_iteration)

    with eventstats.BenchmarkLogSpan("np.ones"):
        x = np.ones(n)
        ray.put(x)

    print "BENCHMARK_STATS:", json.dumps({
        "config": { "scale" : n },
        "events" : eventstats.log_span_events() })
