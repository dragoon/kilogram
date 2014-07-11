#!/usr/bin/env python

import sys
import nltk
import anydbm

# Open just for read
dbpediadb = anydbm.open('dbpedia.dbm', 'r')

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    words = ngram.split()
    for i in range(len(words), 0, -1):
        stop = 0
        for j, ngram in enumerate(nltk.ngrams(words, i)):
            ngram_joined = ' '.join(ngram)
            if ngram_joined in dbpediadb:
                stop = True
                print '%s\t%s' % (dbpediadb[ngram_joined], num)
        if stop:
            break

dbpediadb.close()