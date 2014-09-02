#!/usr/bin/env python

from __future__ import division
import sys

current_ngram = None
cur_values = []


def process_values(values):
    orig_ngram = None
    entity_counts = []
    # prevent zero division
    non_entity_counts = []
    for value in values:
        ngram_type, num = value.split('|--|')
        if ngram_type == 'lower':
            non_entity_counts.append(int(num))
        else:
            orig_ngram = ngram_type
            entity_counts.append(int(num))
    if orig_ngram:
        if not non_entity_counts:
            non_entity_counts = [0.01]
        print '%s\t%s' % (orig_ngram, sum(entity_counts)/sum(non_entity_counts))


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    ngram, value = line.split('\t')

    if not current_ngram:
        current_ngram = ngram

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if ngram == current_ngram:
        cur_values.append(value)
    else:
        process_values(cur_values)
        cur_values = [value]
        current_ngram = ngram

process_values(cur_values)
