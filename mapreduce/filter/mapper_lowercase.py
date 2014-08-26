#!/usr/bin/env python

import sys

# Open just for read
dbpediadb = open('dbpedia_labels.txt')
dbpediadb_lower = {}
for line in dbpediadb:
    label_lower = line.strip().lower()
    if label_lower in dbpediadb_lower:
        dbpediadb_lower[label_lower].add(line.strip())
    else:
        dbpediadb_lower[label_lower] = set(line.strip())
dbpediadb.close()

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

