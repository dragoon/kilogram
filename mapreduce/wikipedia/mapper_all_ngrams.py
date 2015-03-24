#!/usr/bin/env python

import sys
import nltk
import os
import shelve
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)


def merge_titlecases(tokens):
    new_tokens = []
    last_title = False
    for token in tokens:
        if token[0].isupper():
            if last_title:
                new_tokens[-1] += ' ' + token
            else:
                new_tokens.append(token)
            last_title = True
        else:
            new_tokens.append(token)
            last_title = False
    return new_tokens


dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')

for line in sys.stdin:
    if not line:
        continue

    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        tokens_types, tokens_plain = parse_types_text(sentence, dbpedia_types, type_level=-1)

        # do not split title-case sequences
        tokens_plain = merge_titlecases(tokens_plain)
        tokens_types = merge_titlecases(tokens_types)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_plain, n):
                ngram, markers = zip(*ngram)
                count = 1
                print '%s\t%s' % (' '.join(ngram), count)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_types, n):
                ngram, _ = zip(*ngram)
                ngram_joined = ' '.join(ngram)
                if '<dbpedia:' in ngram_joined:
                    print '%s\t%s' % (ngram_joined, 1)


dbpedia_types.close()