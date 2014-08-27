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
    uri_ngram = ngram.replace(' ', '_')

    if uri_ngram in dbpediadb:
        print '%s\t%s|--|%s' % (uri_ngram.lower(), dbpediadb[uri_ngram]['uri'], num)

    if uri_ngram in dbpediadb_lower_set:
        print '%s\t%s|--|%s' % (uri_ngram.lower(), 'lower', num)

dbpediadb.close()
