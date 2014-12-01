#!/usr/bin/env python

import sys
from kilogram.lang import number_replace
import re
MULTI_PUNCT_RE = re.compile(r'[^a-zA-Z0-9_\s\-]{2,}')


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    if MULTI_PUNCT_RE.search(orig_ngram):
        continue

    new_words = []
    for word in orig_ngram.split():
        if (word.startswith('<dbpedia:') and word.endswith('>')) or word in ('<PERSON>', '<CITY>'):
            new_words.append(word)
        else:
            # numeric replace
            # TODO: to lower or not to lower? That is the question.
            new_words.append(number_replace(word.lower()))

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
