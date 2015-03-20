#!/usr/bin/env python

import sys
from kilogram.types.prediction import  TypeDataPacker

current_ngram = None
ngram = None
cur_counts = []

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    try:
        ngram, entity_type, count = line.split('\t')
    except:
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if ngram == current_ngram:
        cur_counts.append((entity_type, count))
    else:
        if current_ngram:
            # write result to STDOUT
            print '%s\t%s' % (current_ngram, TypeDataPacker.pack(cur_counts))
        cur_counts = [(entity_type, count)]
        current_ngram = ngram

# do not forget to output the last word if needed!
if ngram and current_ngram == ngram:
    print '%s\t%s' % (ngram, TypeDataPacker.pack(cur_counts))