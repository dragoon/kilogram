#!/usr/bin/env python
"""Mapper to strip determiners"""

import sys
from kilogram.lang import strip_determiners

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    new_ngram = strip_determiners(ngram)
    if new_ngram == ngram:
        print line
    elif new_ngram in ngram:  # means we stripped only at start/end positions
        continue
    else:
        print '%s\t%s' % (new_ngram, num)




