#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import sys
import shelve
import anydbm
import nltk

from kilogram.dataset.wikipedia.entities import parse_types_text

dbpedia_redirects = anydbm.open('dbpedia_redirects.dbm', 'r')
dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')


for line in sys.stdin:
    if not line:
        continue
    line = parse_types_text(line, dbpedia_redirects, dbpedia_types)
    for sentence in line.split(' . '):
        if not sentence.endswith('.'):
            sentence += ' .'
        words = sentence.split()
        for ngram in nltk.ngrams(words, 3):
            ngram_joined = ' '.join(ngram)
            if '<dbpedia:' in ngram_joined:
                print '%s\t%s' % (ngram_joined, 1)
                for n in (1, 2):
                    for lower_ngram in nltk.ngrams(ngram, n):
                        print '%s\t%s' % (' '.join(lower_ngram), 1)

dbpedia_redirects.close()
dbpedia_types.close()
