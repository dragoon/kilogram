#!/usr/bin/env python

from collections import defaultdict
import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm', flag='r')
dbpediadb_lower = defaultdict(list)
for key in dbpediadb.iterkeys():
    dbpediadb_lower[key.lower()].append(key)

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    uri_ngram = ngram.replace(' ', '_')

    if uri_ngram in dbpediadb:
        print '%s\t%s|--|%s' % (uri_ngram, 'orig', num)

    if uri_ngram in dbpediadb_lower:
        for key in dbpediadb_lower[uri_ngram]:
            print '%s\t%s|--|%s' % (key, 'lower', num)

dbpediadb.close()