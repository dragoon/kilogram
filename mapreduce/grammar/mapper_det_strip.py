#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import os
import sys

DT_STRIPS = {'my', 'our', 'your', 'their', 'a', 'an', 'the', 'her'}
FILTER = None
FILTER_FILE = os.environ.get('FILTER_FILE')
if FILTER_FILE:
    FILTER = set(open(FILTER_FILE).read().splitlines())


for line in sys.stdin:
    line = line.strip().lower()
    ngram, num = line.split('\t')
    words = ngram.split()
    if len(words) < 3:
        continue
    dt_index = [str(i) for i, word in enumerate(words) if word in DT_STRIPS]
    if not dt_index:
        continue
    if set(dt_index).intersection({'0', str(len(words)-1)}):
        continue

    if FILTER is not None:
        if not FILTER.intersection(words):
            continue

    new_words = [w for w in words if w not in DT_STRIPS]
    ngram = ' '.join(new_words)

    print '%s\t%s\t%s' % (ngram, ','.join(dt_index), num)