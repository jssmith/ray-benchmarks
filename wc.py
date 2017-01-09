import sys
import re

from collections import defaultdict

from utils import Timer
from sweep import sweep_iterations

import event_stats

def wc(input_files):
    word_counters = defaultdict(lambda: 0)
    for input_file in input_files:
        print "counting {}".format(input_file)
        with open(input_file, 'r') as f:
            for line in f:
                line = re.sub(r"[^a-zA-Z]", " ", line)
                line = re.sub(r"  +", " ", line)
                for word in filter(lambda x: len(x) > 0, map(lambda x: x.lower(), line.split(' '))):
                    word_counters[word] += 1
    return word_counters

def do_wc(input_files):
    with event_stats.benchmark_measure_noray():
        word_frequencies = wc(input_files)

        most_popular_word = None
        most_popular_ct = 0
        total_word_chars = 0
        for word, ct in word_frequencies.items():
                total_word_chars += len(word)
                if ct > most_popular_ct:
                    most_popular_word = word
                    most_popular_ct = ct
        print "most popular word is '{}' with count {}".format(most_popular_word, most_popular_ct)
        print "dict has {} items with total key length {}".format(len(word_frequencies), total_word_chars)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: wc.py input_file [input_file ...]"
    input_files = sys.argv[1:]
    for _ in range(sweep_iterations):
        do_wc(input_files)

    config_info = {
        'benchmark_name' : 'wc',
        'benchmark_implementation' : '1-thread',
        'benchmark_iterations' : sweep_iterations,
        'input_file_base' : input_files[0],
        'num_inputs' : len(input_files)
    }
    event_stats.print_stats_summary_noray(config_info)
