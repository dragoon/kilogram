#!/usr/bin/env python

import sys

current_ngram = None
ngram = None
cur_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    try:
        ngram, w_count = line.rsplit('\t', 1)
    except:
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if ngram == current_ngram:
        cur_count += int(w_count)
    else:
        if current_ngram:
            # write result to STDOUT
            print '%s\t%s' % (current_ngram, cur_count)
        cur_count = int(w_count)
        current_ngram = ngram

# do not forget to output the last word if needed!
if ngram and current_ngram == ngram:
    print '%s\t%s' % (ngram, cur_count)