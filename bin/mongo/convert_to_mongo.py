#!/usr/bin/env python
"""
Run as: cat google_ngram_file | ./convert_to_mongo.py --sub subs_file.txt | sort -k1,1 -t $'\t' > output
Then proceed to mongodb import
"""
import sys
import argparse

from .ngram_service import SUBSTITUTION_TOKEN
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--sub', dest='subst_file', action='store',
                   help='path to sustitutions file')
args = parser.parse_args()
SUBST_SET = set(open(args.subst_file).read().split('\n'))

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    ngram, count = line.split('\t')
    words = ngram.split()
    for i, word in enumerate(words):
        if word in SUBST_SET:
            ngram = words[:]
            ngram[i] = SUBSTITUTION_TOKEN
            ngram.append(str(i))
            print '\t'.join([' '.join(ngram), word, count])
