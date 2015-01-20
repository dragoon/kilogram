#!/usr/bin/env python

import sys
import shelve
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text
from kilogram.lang.tokenize import wiki_tokenize_func

try:
    dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')
except:
    # used to generate plain n-grams
    dbpedia_types = {}

for line in sys.stdin:
    if not line:
        continue
    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        # -1 = most generic, 0 = most specific
        print parse_types_text(sentence, dbpedia_types, numeric=False, type_level=-1)

if dbpedia_types:
    dbpedia_types.close()