from subprocess import Popen, PIPE
import re
import sys
import math

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
    am = re.findall('^RAY_BENCHMARK_([A-Z_]+): (\d+\.\d+)$', stdoutdata, re.MULTILINE)
    elapsed_times = []
    for m in am:
        # result_type = m[0]
        elapsed_times.append(float(m[1]))
    return elapsed_times

def log_result(benchmark_name, scale, times):
    with open('sweep_log.csv', 'a') as f:
        for time in times:
            f.write('{},{:d},{:f}\n'.format(benchmark_name, scale, time))

def sort_benchmark(num_partitions, partition_size, input_prefix, filename_format_str):
    num_records = num_partitions * partition_size
    times = run_ray_benchmark('sort_ray_np.py', num_partitions, num_partitions, input_prefix, filename_format_str)
    print '{} {}'.format(n, str(times))
    log_result('sort_ray_np', num_records, times)

    times = run_split_benchmark('sort_1d_np.py', num_partitions, input_prefix, filename_format_str)
    print '{} {}'.format(num_partitions, str(times))
    log_result('sort_1d_np', num_records, times)

    times = run_serial_benchmark('sort_serial_np.py', num_partitions, input_prefix, filename_format_str)
    print '{} {}'.format(num_partitions, str(times))
    log_result('sort_serial_np', num_records, times)

def wc_benchmark(num_partitions, partition_size, input_prefix, filename_format_str):
    num_records = num_partitions * partition_size
    times = run_ray_benchmark('wc_ray.py', num_partitions, num_partitions, input_prefix, filename_format_str)
    print '{} {}'.format(num_partitions, str(times))
    log_result('wc_ray', num_records, times)

    times = run_serial_benchmark('wc.py', num_partitions, input_prefix, filename_format_str)
    print '{} {}'.format(num_partitions, str(times))
    log_result('wc', num_records, times)

def arithmetic_progression(start_str, end_str, step_str):
    return range(int(start_str), int(end_str) + 1, int(step_str))

def geometric_progression(start_str, end_str, step_str):
    start = float(start_str)
    end = float(end_str)
    step = float(step_str)
    progression = []
    present_value = start
    previous_value = None
    while present_value <= end:
        if previous_value is None or int(present_value) != int(previous_value):
            progression.append(int(present_value))
        previous_value = present_value
        present_value *= step
    return progression

benchmarks = {
    'sort' : sort_benchmark,
    'wc' : wc_benchmark
}

progressions = {
    'arithmetic' : arithmetic_progression,
    'geometric' : geometric_progression
}

if __name__ == '__main__':
    if len(sys.argv) != 9:
        print "Usage: sweep.py progression start end step benchmark partition_size input_prefix file_format"
        sys.exit(1)
    progression = progressions[sys.argv[1]]
    start_str = sys.argv[2]
    end_str = sys.argv[3]
    step_str = sys.argv[4]
    benchmark = benchmarks[sys.argv[5]]
    partition_size = int(sys.argv[6])
    input_prefix = sys.argv[7]
    file_format = sys.argv[8]
    if file_format == 'text':
        filename_format_str = "{}_{:03d}"
    elif file_format == 'numpy':
        filename_format_str = "{}_{:03d}.npy"
    else:
        print "File format must be either 'text' or 'numpy'"
        sys.exit(1)
    for n in progression(start_str, end_str, step_str):
        benchmark(n, partition_size, input_prefix, filename_format_str)
