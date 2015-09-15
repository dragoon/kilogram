#!/usr/bin/env python
"""
cat wekex_test_plain | python ../notebooks/kilogram/mapreduce/wikipedia/resolve_entities.py > wekex_test_linked
"""

import sys
from kilogram.dataset.dbpedia import NgramEntityResolver

ner = NgramEntityResolver("dbpedia_types.txt", "dbpedia_uri_excludes.txt", "dbpediadb_lower.txt", "dbpedia_ontology.owl")

for line in sys.stdin:
    line = line.strip()
    # split the line into words
    new_words = ner.resolve_entities(line.split())
    new_line = ' '.join(new_words)
    print new_line
