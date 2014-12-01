#!/usr/bin/env python
"""Filters determiners and forms new n-grams with skips"""

import os
import sys

DT_STRIPS = {'my', 'our', 'your', 'their', 'a', 'an', 'the', 'her', 'its'}

FILTER = None
FILTER_FILE = os.environ.get('FILTER_FILE')
if FILTER_FILE:
    FILTER = set(open(FILTER_FILE).read().splitlines())


for line in sys.stdin:
    line = line.strip().lower()
    ngram, num = line.split('\t')
    words = ngram.split()

    dt_index = [i for i, word in enumerate(words) if word in DT_STRIPS]

    # Only one DT and not on the last position
    if not dt_index or len(dt_index) > 1 or dt_index[0] == len(words)-1:
        continue
    words[dt_index[0]] = 'DT'

    if FILTER is not None:
        if len(words) not in {3, 4}:
            continue
        if not FILTER.intersection(words):
            continue
    else:
        if len(words) not in {2, 3}:
            continue

    print '%s\t%s' % (' '.join(words), num)