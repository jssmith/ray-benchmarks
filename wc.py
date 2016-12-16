import sys
import re

from collections import defaultdict

def wc(input_file):
    word_counters = defaultdict(lambda: 0)
    with open(input_file, 'r') as f:

        for line in f:
            line = re.sub(r"[^a-zA-Z]", " ", line)
            line = re.sub(r"  +", " ", line)
            for word in filter(lambda x: len(x) > 0, map(lambda x: x.lower(), line.split(' '))):
                word_counters[word] += 1
        return word_counters

if __name__ == '__main__':
    input_file = sys.argv[1]
    word_frequencies = wc(input_file)

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
