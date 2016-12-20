from subprocess import Popen, PIPE
import re
import sys

def run_serial_benchmark(program, num_splits, prefix):
    args = ['python', program] + ['{}_{:03d}'.format(prefix, i) for i in range(num_splits)]
    return run_benchmark(args)

def run_ray_benchmark(program, num_workers, num_splits, prefix):
    args = ['python', program,
        str(num_workers),
        str(num_splits)] + ['{}_{:03d}'.format(prefix, i) for i in range(num_splits)]
    return run_benchmark(args)

def run_benchmark(args):
    proc = Popen(args, stdout=PIPE, stderr=PIPE)
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
    if len(sys.argv) != 4:
        print "Usage: sweep.py start end input_prefix"
        sys.exit(1)
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    input_prefix = sys.argv[3]
    for n in range(start, end):
        time = run_ray_benchmark('sort_ray.py', n, n, input_prefix)
        print '{} {}'.format(n, time)
        log_result('sort_ray', n * 100000, time)
        time = run_serial_benchmark('sort_serial.py', n, input_prefix)
        print '{} {}'.format(n, time)
        log_result('sort_serial', n * 100000, time)
