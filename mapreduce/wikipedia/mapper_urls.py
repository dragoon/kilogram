#!/usr/bin/env python

import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    uri_counts = line.split('\t')[1]
    for uri_count in uri_counts.split():
        print '%s\t%s' % tuple(uri_count.split(','))
