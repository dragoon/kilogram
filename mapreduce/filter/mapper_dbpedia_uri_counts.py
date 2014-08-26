#!/usr/bin/env python

import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm')

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    uri_ngram = ngram.replace(' ', '_')
    if uri_ngram in dbpediadb:
        # put canonical url
        print '%s\t%s' % (dbpediadb[uri_ngram], num)

dbpediadb.close()
