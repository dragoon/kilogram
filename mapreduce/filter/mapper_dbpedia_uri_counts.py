#!/usr/bin/env python

import sys
import nltk

# Open just for read
dbpediadb = set(open('dbpedia_labels.txt').read().splitlines())

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    if ngram in dbpediadb:
        print '<%s>\t%s' % (ngram.replace(' ', '_'), num)

