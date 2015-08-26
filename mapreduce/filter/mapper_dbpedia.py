#!/usr/bin/env python

import sys
from kilogram.dataset.dbpedia import NgramEntityResolver

ner = NgramEntityResolver("dbpedia_types.txt", "dbpedia_uri_excludes.txt", "dbpediadb_lower.txt")

for line in sys.stdin:
    if not line.strip():
        print line
        continue
    # split the line into words
    new_words = ner.resolve_entities(line.split())
    new_line = ' '.join(new_words)
    print new_line
