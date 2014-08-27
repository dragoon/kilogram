#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import sys
import nltk
import os

from kilogram.dataset.wikipedia.entities import parse_types_text

N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)

dbpedia_types = {}
try:
    for line in open('dbpedia_types.tsv'):
        label, uri, types = line.strip().split('\t')
        dbpedia_types[label] = types.split(';')
except:
    # used to generate plain n-grams
    dbpedia_types = {}


for line in sys.stdin:
    if not line:
        continue
    line = parse_types_text(line, dbpedia_types, numeric=False)
    sentences = line.split(' . ')
    last = len(sentences) - 1
    for i, sentence in enumerate(sentences):
        try:
            unicode(sentence)
        except UnicodeDecodeError:
            continue
        if i == last and not sentence.endswith('.'):
            continue
        if not sentence.endswith('.'):
            sentence += ' .'
        words = sentence.split()
        for n in range(1,N+1):
            for ngram in nltk.ngrams(words, n):
                ngram_joined = ' '.join(ngram)
                print '%s\t%s' % (ngram_joined, 1)
