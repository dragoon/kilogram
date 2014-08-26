#!/usr/bin/env python

import sys

# Open just for read
dbpediadb_lower = set(x.lower() for x in open('dbpedia_labels.txt').read().splitlines())

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    if ngram.lower() in dbpediadb_lower:
        if ngram == ngram.lower():
            print '%s\t%s|--|%s' % (ngram.lower(), 'lower', num)
        else:
            print '%s\t%s|--|%s' % (ngram.lower(), ngram.replace(' ', '_'), num)

