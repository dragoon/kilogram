#!/usr/bin/env python

import sys
from collections import defaultdict
from kilogram.ngram_service import ListPacker

current_ngram = None
ngram = None
cur_counts = defaultdict(lambda: 0)

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    try:
        ngram, subcount_key_count = line.split('\t')
        subcount_key, count = subcount_key_count.rsplit(' ', 1)
    except:
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if ngram == current_ngram:
        cur_counts[subcount_key] += int(count)
    else:
        if current_ngram:
            # write result to STDOUT
            print '%s\t%s' % (current_ngram, ListPacker.pack(cur_counts.items()))
        cur_counts = defaultdict(lambda: 0)
        cur_counts[subcount_key] += int(count)
        current_ngram = ngram

# do not forget to output the last word if needed!
if ngram and current_ngram == ngram:
    print '%s\t%s' % (ngram, ListPacker.pack(cur_counts.items()))
