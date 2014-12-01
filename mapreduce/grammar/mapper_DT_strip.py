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
    if not dt_index or len(dt_index) > 1:
        continue
    words[dt_index[0]] = 'DT'

    allowed_indexes = [0, 1]

    if FILTER is not None:
        if len(words) not in {3, 4}:
            continue
        if dt_index[0] not in [x+1 for x in allowed_indexes]:
            continue
        sub_index = [i for i, word in enumerate(words) if word in FILTER]
        if not sub_index or len(sub_index) > 1:
            continue
        if sub_index[0] not in allowed_indexes:
            continue
    else:
        if dt_index[0] not in allowed_indexes:
            continue
        if len(words) not in {2, 3}:
            continue

    print '%s\t%s' % (' '.join(words), num)