from subprocess import Popen, PIPE
import re
import sys

def run_serial_benchmark(program, num_splits, prefix, filename_format_str):
    args = ['python', program] + [filename_format_str.format(prefix, i) for i in range(num_splits)]
    return run_benchmark(args)

def run_split_benchmark(program, num_splits, prefix, filename_format_str):
    args = ['python', program, 
        str(num_splits)] + [filename_format_str.format(prefix, i) for i in range(num_splits)]
    return run_benchmark(args)

def run_ray_benchmark(program, num_workers, num_splits, prefix, filename_format_str):
    args = ['python', program,
        str(num_workers),
        str(num_splits)] + [filename_format_str.format(prefix, i) for i in range(num_splits)]
    return run_benchmark(args)

def run_benchmark(args):
    proc = Popen(args, stdout=PIPE, stderr=PIPE)
    (stdoutdata, stderrdata) = proc.communicate()
    returncode = proc.returncode
    print 'COMMAND'
    print ' '.join(args)
    print 'STDOUT'
    print stdoutdata
    print 'STDERR'
    print stderrdata
    m = re.search('^RAY_BENCHMARK_([A-Z_]+): (\d+\.\d+)$', stdoutdata, re.MULTILINE)
    result_type = m.group(1)
    elapsed_time = float(m.group(2))
    return elapsed_time

def log_result(benchmark_name, scale, time):
    with open('sweep_log.csv', 'a') as f:
        f.write('{},{:d},{:f}\n'.format(benchmark_name, scale, time))

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: sweep.py start end input_prefix file_format"
        sys.exit(1)
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    input_prefix = sys.argv[3]
    file_format = sys.argv[4]
    if file_format == 'text':
        filename_format_str = "{}_{:03d}"
    elif file_format == 'numpy':
        filename_format_str = "{}_{:03d}.npy"
    else:
        print "File format must be either 'text' or 'numpy'"
        sys.exit(1)
    partition_size = 1000000
    for n in range(start, end):
        num_records = n * partition_size
        time = run_ray_benchmark('sort_ray_np.py', n, n, input_prefix, filename_format_str)
        print '{} {}'.format(n, time)
        log_result('sort_ray_np', num_records, time)

        time = run_split_benchmark('sort_1d_np.py', n, input_prefix, filename_format_str)
        print '{} {}'.format(n, time)
        log_result('sort_1d_np', num_records, time)

        time = run_serial_benchmark('sort_serial_np.py', n, input_prefix, filename_format_str)
        print '{} {}'.format(n, time)
        log_result('sort_serial_np', num_records, time)
