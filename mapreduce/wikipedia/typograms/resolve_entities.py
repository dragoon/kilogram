#!/usr/bin/env python
"""
cat wekex_test_plain | python ../notebooks/kilogram/mapreduce/wikipedia/resolve_entities.py > wekex_test_linked
"""

import sys
from kilogram.dataset.dbpedia import NgramEntityResolver

ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")

for line in sys.stdin:
    line = line.strip()
    # split the line into words
    new_words = ner.resolve_entities(line.split())
    new_line = ' '.join(new_words)
    print new_line
