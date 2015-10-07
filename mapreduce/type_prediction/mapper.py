#!/usr/bin/env python
"""Mapper to filter and extract types"""

import sys
from kilogram.ngram_service import SUBSTITUTION_TOKEN

for line in sys.stdin:
    # split the line into words
    ngram, num = line.strip().split('\t')

    if int(num) < 10:
        continue

    type_count = ngram.count('<dbpedia:')
    ngram = ngram.split()
    if type_count > 0:
        type_indexes = [i for i, x in enumerate(ngram) if x.startswith('<dbpedia:')]
        for type_index in type_indexes:
            subst_ngram = ngram[:]
            entity_type = ngram[type_index]
            subst_ngram[type_index] = SUBSTITUTION_TOKEN
            new_ngram = " ".join(subst_ngram)
            print '%s\t%s\t%s' % (new_ngram, entity_type, num)
