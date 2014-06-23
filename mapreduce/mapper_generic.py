#!/usr/bin/env python

import sys
import string

from kilogram.lang import RE_NUM_SUBS

MY_PRINTABLE = set(string.letters+string.digits+string.punctuation+' ')

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
        for repl, regex in RE_NUM_SUBS:
            word1 = regex.sub(repl, word)
            if word1 != word:
                break
        new_words.append(word1)
    ngram = ' '.join(new_words)

    print '%s\t%s' % (ngram, num)
