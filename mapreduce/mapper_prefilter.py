#!/usr/bin/env python

import sys
import re
import string

MY_PRINTABLE = set(string.letters+string.digits+string.punctuation+' ')
MULTI_PUNCT_RE = re.compile(r'(^| )\W+ \W+($| )')


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    if not MY_PRINTABLE.issuperset(orig_ngram):
        continue
    # skip POS tags
    is_pos = [1 for word in orig_ngram.split() if '_' in word and word != '_']
    if is_pos:
        continue

    if MULTI_PUNCT_RE.search(orig_ngram):
        continue

    # percentages as well
    orig_ngram = orig_ngram.replace(" %", "%")

    print '%s\t%s' % (orig_ngram, num)
