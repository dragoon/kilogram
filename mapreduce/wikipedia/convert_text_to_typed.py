#!/usr/bin/env python

import sys
import shelve
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

try:
    dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')
except:
    # used to generate plain n-grams
    dbpedia_types = {}

for line in sys.stdin:
    if not line:
        continue
    line = parse_types_text(line, dbpedia_types, numeric=False)
    for sentence in line_filter(line):
        print sentence

if dbpedia_types:
    dbpedia_types.close()