#!/usr/bin/env python

import sys
FILTER_FILE = 'words.txt'

FILTER = set(open(FILTER_FILE).read().splitlines())


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    ngram, num = line.split('\t')
    words = ngram.split()
    if not FILTER.intersection(words):
        continue

    print '%s\t%s' % (ngram, num)
