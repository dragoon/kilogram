#!/usr/bin/env python

import sys

# Open just for read
dbpediadb_lower = dict((x.lower(), set()) for x in open('dbpedia_labels.txt').read().splitlines())
for label in open('dbpedia_labels.txt'):
    dbpediadb_lower[label.lower()].add(label)

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    label_set = dbpediadb_lower.get(ngram.lower(), set())
    if ngram in label_set:
        print '%s\t%s|--|%s' % (ngram.lower(), ngram.replace(' ', '_'), num)
    if ngram in dbpediadb_lower:
        print '%s\t%s|--|%s' % (ngram.lower(), 'lower', num)

