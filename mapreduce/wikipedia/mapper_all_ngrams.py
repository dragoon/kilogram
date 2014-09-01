#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import sys
import nltk
import os
import shelve
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)

dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')

for line in sys.stdin:
    if not line:
        continue

    line_types = parse_types_text(line, dbpedia_types, numeric=False)
    line_plain = parse_types_text(line, {}, numeric=False)
    for sentence in line_filter(line_plain):
        words = sentence.split()
        for n in range(1, N+1):
            for ngram in nltk.ngrams(words, n):
                ngram_joined = ' '.join(ngram)
                print '%s\t%s' % (ngram_joined, 1)
    for sentence in line_filter(line_types):
        words = sentence.split()
        for n in range(1, N+1):
            for ngram in nltk.ngrams(words, n):
                ngram_joined = ' '.join(ngram)
                if '<dbpedia:' in ngram_joined:
                    print '%s\t%s' % (ngram_joined, 1)



dbpedia_types.close()