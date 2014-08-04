#!/usr/bin/env python

import sys
import os
import nltk
from kilogram.lang import number_replace

FILTER_FILE = os.environ['FILTER_FILE']
FILTER = set([x[29:-1].replace('_', ' ') for x in open(FILTER_FILE).read().splitlines()])

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')

    words = orig_ngram.split()
    stop = 0
    for i in range(len(words), 0, -1):
        for j, ngram in enumerate(nltk.ngrams(words, i)):
            if ' '.join(ngram) in FILTER:
                stop = 1
                break
        if stop:
            break
    if stop:
        continue

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
                # TODO: to lower or not to lower? That is the question.
                new_words.append(word)

    orig_ngram = ' '.join(new_words)
    # replace apostrophes without duplicating
    if "'" in orig_ngram:
        orig_ngram = orig_ngram.replace(" '", "'")
        orig_ngram = orig_ngram.replace("' ", "'")

    ngrams = {orig_ngram}

    if '-' in orig_ngram:
        ngram = orig_ngram.replace(' -', '-')
        ngram = ngram.replace('- ', '-')
        ngrams.add(ngram)

    for ngram in ngrams:
        print '%s\t%s' % (ngram, num)
