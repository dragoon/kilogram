#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import sys
import nltk
import os
import shelve
from kilogram.lang.tokenize import wiki_tokenize_func
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

    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        tokens_types, tokens_plain = parse_types_text(sentence, dbpedia_types, numeric=False, type_level=-1)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_plain, n):
                ngram, markers = zip(*ngram)
                count = 1
                if 1 in markers:
                    count = 0.5
                print '%s\t%s' % (' '.join(ngram), count)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_types, n):
                ngram, _ = zip(*ngram)
                ngram_joined = ' '.join(ngram)
                if '<dbpedia:' in ngram_joined:
                    print '%s\t%s' % (ngram_joined, 0.5)


dbpedia_types.close()