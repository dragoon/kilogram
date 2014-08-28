#!/usr/bin/env python

import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm', flag='r')
dbpediadb_lower = {}
for key, value in dbpediadb.iteritems():
    if key.lower() not in dbpediadb_lower:
        dbpediadb_lower[key.lower()] = {'uri': value['uri'], 'labels': {key}}
    else:
        dbpediadb_lower[key.lower()]['labels'].add(key)

dbpediadb.close()

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    uri_ngram = ngram.replace(' ', '_')

    if uri_ngram in dbpediadb_lower:
        print '%s\t%s|--|%s' % (dbpediadb_lower[uri_ngram]['uri'].lower(), 'lower', num)
    elif uri_ngram.lower() in dbpediadb_lower:
        entity = dbpediadb_lower[uri_ngram.lower()]
        if uri_ngram in entity['labels']:
            print '%s\t%s|--|%s' % (entity['uri'].lower(), entity['uri'], num)
