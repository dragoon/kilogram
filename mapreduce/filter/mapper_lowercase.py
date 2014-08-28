#!/usr/bin/env python

import sys
import shelve

# Open just for read
dbpedia_shelve = shelve.open('dbpedia_types.dbm', flag='r')
dbpediadb_lower = dict([(key.lower(), value['uri']) for key, value in dbpedia_shelve.items()])
dbpediadb = dict([(key, value['uri']) for key, value in dbpedia_shelve.items()])
dbpedia_shelve.close()

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    uri_ngram = ngram.replace(' ', '_')

    if uri_ngram in dbpediadb:
        print '%s\t%s|--|%s' % (dbpediadb[uri_ngram].lower(), dbpediadb[uri_ngram], num)

    if uri_ngram in dbpediadb_lower:
        print '%s\t%s|--|%s' % (dbpediadb_lower[uri_ngram].lower(), 'lower', num)

