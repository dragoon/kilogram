#!/usr/bin/env python

import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    uri_count = line.split('\t')[1]
    print '%s\t%s' % (uri_count.split())
