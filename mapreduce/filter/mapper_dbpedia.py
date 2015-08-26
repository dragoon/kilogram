#!/usr/bin/env python

import sys
from kilogram.dataset.dbpedia import NgramEntityResolver

ner = NgramEntityResolver("dbpedia_types.txt", "dbpedia_uri_excludes.txt", "dbpediadb_lower.txt")

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    new_words = ner.resolve_entities(orig_ngram.split())
    new_ngram = ' '.join(new_words)

    if new_ngram != orig_ngram:
        print '%s\t%s' % (new_ngram.strip(), num)
