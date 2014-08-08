#!/usr/bin/env python

import sys
import nltk

# Open just for read
dbpediadb = set(open('dbpedia_labels.txt').read().splitlines())

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
                print '<%s>\t%s' % (ngram_joined.replace(' ', '_'), num)
        if stop:
            break