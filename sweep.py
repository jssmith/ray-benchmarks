from subprocess import Popen, PIPE
import re

def run_benchmark(program, num_workers, num_splits, prefix):
    proc = Popen(['python', program,
        str(num_workers),
        str(num_splits)] + ['{}_{:03d}'.format(prefix, i) for i in range(num_splits)],
        stdout=PIPE, stderr=PIPE)
    (stdoutdata, stderrdata) = proc.communicate()
    returncode = proc.returncode
    print stdoutdata
    m = re.search('^RAY_BENCHMARK_([A-Z_]+): (\d+\.\d+)$', stdoutdata, re.MULTILINE)
    result_type = m.group(1)
    elapsed_time = float(m.group(2))
    return elapsed_time

def log_result(benchmark_name, scale, time):
    with open('sweep_log.csv', 'a') as f:
        f.write('{},{:d},{:f}\n'.format(benchmark_name, scale, time))

if __name__ == '__main__':
    for n in range(1,4):
        time = run_benchmark('sort_ray.py', n, n, 'sort_test')
        print '{} {}'.format(n, time)
        log_result('sort', n * 100000, time)
