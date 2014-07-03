#!/usr/bin/env python

import sys
import os
FILTER_FILE = 'words.txt'

FILTER = set(open(FILTER_FILE).read().splitlines())
N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    ngram, num = line.split('\t')
    words = ngram.split()
    if len(words) != N:
        continue
    if not FILTER.intersection(words):
        continue

    print '%s\t%s' % (ngram, num)
