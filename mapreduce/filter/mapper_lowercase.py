#!/usr/bin/env python

import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm', flag='r')
dbpediadb_lower_set = set([x.lower() for x in dbpediadb.keys()])

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    if ngram in dbpediadb:
        print '%s\t%s|--|%s' % (ngram.lower(), dbpediadb[ngram]['uri'], num)

    if ngram in dbpediadb_lower_set:
        print '%s\t%s|--|%s' % (ngram.lower(), 'lower', num)

dbpediadb.close()