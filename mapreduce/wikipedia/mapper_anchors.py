#!/usr/bin/env python

import sys
import re

ENTITY_MATCH_RE = re.compile(r'<(.+)\|(.+)>')

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    for word in line.split():
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            anchor_text = match.group(2).lower()
            anchor_text = anchor_text.replace('_', ' ')
            print '%s\t%s\t%s' % (anchor_text, uri, "1")
