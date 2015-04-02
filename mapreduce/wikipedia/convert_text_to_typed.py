#!/usr/bin/env python

import sys
import os
import shelve
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text
from kilogram.lang.tokenize import wiki_tokenize_func

TYPE_LEVEL = os.environ.get('TYPE_LEVEL', None)
if TYPE_LEVEL is not None:
    TYPE_LEVEL = int(TYPE_LEVEL)

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
        words = []
        for word in parse_types_text(sentence, dbpedia_types)[0]:
            if word.startswith('<dbpedia:'):
                entity_types = word.split(',')
                words.append(entity_types[TYPE_LEVEL])
            else:
                words.append(word)
        print ' '.join(words)

if dbpedia_types:
    dbpedia_types.close()