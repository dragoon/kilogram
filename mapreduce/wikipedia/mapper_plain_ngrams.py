#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import sys
import nltk
import os
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)

for line in sys.stdin:
    if not line:
        continue
    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        words = zip(*parse_types_text(sentence, {})[0])[0]
        for n in range(1, N+1):
            for ngram in nltk.ngrams(words, n):
                ngram_joined = ' '.join(ngram)
                print '%s\t%s' % (ngram_joined, 1)
