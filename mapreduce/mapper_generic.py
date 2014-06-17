#!/usr/bin/env python

import re
import sys
import string

MY_PRINTABLE = set(string.letters+string.digits+string.punctuation+' ')

# we ensure it starts with digit, since otherwise we will detect NUMBER after the dot anyway
FLOAT_REGEX = r'(?:[1-9]\d*|0)(?:[\.,]\d+)?'

PERCENT_RE = re.compile(r'\b\d{1,2}([\.,]\d{1,2})?\%(\s|$)')
NUM_RE = re.compile(FLOAT_REGEX)
TIME_RE1 = re.compile(r'\b\d{1,2}:\d{2}\b')
TIME_RE2 = re.compile(r'\b[01]\d(?:[:\.][0-5]\d)?(a\.m\.|p\.m\.|am|pm)(\s|$)')
# we need to separate square and volume, otherwise they will be mixed
VOL_RE = re.compile(r'\b{0}m3(\s|$)'.format(FLOAT_REGEX))
SQ_RE = re.compile(r'\b{0}m2(\s|$)'.format(FLOAT_REGEX))

RE_SUBS = [('AREA', SQ_RE), ('VOL', VOL_RE),
           ('PERCENT', PERCENT_RE), ('TIME1', TIME_RE1), ('TIME2', TIME_RE2), ('NUM', NUM_RE)]

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip().lower()
    # split the line into words
    ngram, num = line.split('\t')
    if not MY_PRINTABLE.issuperset(ngram):
        continue
    # skip POS tags
    words = ngram.split()
    is_pos = [1 for word in words if '_' in word and word != '_']
    if is_pos:
        continue

    new_words = []
    for word in words:
        word1 = word
        for repl, regex in RE_SUBS:
            word1 = regex.sub(repl, word)
            if word1 != word:
                break
        new_words.append(word1)
    ngram = ' '.join(new_words)

    print '%s\t%s' % (ngram, num)
