#!/usr/bin/env python

import sys

# Open just for read
dbpediadb = set(open('dbpedia_labels.txt').read().splitlines())
dbpediadb_lower = set(x.lower() for x in open('dbpedia_labels.txt').read().splitlines())

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    if ngram in dbpediadb:
        print '%s\t%s|--|%s' % (ngram.lower(), ngram.replace(' ', '_'), num)
    if ngram in dbpediadb_lower:
        print '%s\t%s|--|%s' % (ngram.lower(), 'lower', num)

