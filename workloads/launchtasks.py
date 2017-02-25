import ray
import raybench
import time

@ray.remote
def donothing():
    pass

def benchmark_launchtasks(num_tasks):
    with raybench.measure():
        start_time = time.time()
        l = [donothing.remote() for _ in range(num_tasks)]
        elapsed_time = time.time() - start_time
        print "launched {} tasks in {:.6f}s for {:d}/s".format(num_tasks, elapsed_time, int(num_tasks / elapsed_time))
    with raybench.cleanup():
        ray.wait(l, num_returns=len(l))

if __name__ == '__main__':
    bench_env = raybench.Env()
    bench_env.ray_init()

    benchmark_launchtasks(bench_env.num_workers * 100)
    benchmark_launchtasks(bench_env.num_workers * 1000)
    benchmark_launchtasks(bench_env.num_workers * 10000)

    ray.flush_log()
