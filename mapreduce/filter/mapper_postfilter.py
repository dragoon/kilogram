#!/usr/bin/env python

import sys
from kilogram.lang import number_replace

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')

    new_words = []
    for word in orig_ngram.split():
        if word.startswith('<') and word.endswith('>'):
            new_words.append(word)
        else:
            # numeric replace
            new_word = number_replace(word)
            if new_word != word:
                new_words.append(new_word)
            else:
                new_words.append(word.lower())

    print '%s\t%s' % (' '.join(new_words), num)
