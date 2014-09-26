#!/usr/bin/env python

from collections import defaultdict
import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm', flag='r')
labels = set(dbpediadb.keys())
dbpediadb.close()
dbpediadb_lower = shelve.open('dbpedia_lowercase2labels.dbm', flag='r')

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    label = ngram.replace(' ', '_')

    if label in labels:
        print '%s\t%s|--|%s' % (label, 'orig', num)

    if label in dbpediadb_lower:
        for key in dbpediadb_lower[label]:
            print '%s\t%s|--|%s' % (key, 'lower', num)

dbpediadb.close()
