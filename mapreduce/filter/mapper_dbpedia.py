#!/usr/bin/env python

import sys
import nltk
import shelve

# Open just for read
dbpedia_typesdb = shelve.open('dbpedia_types.dbm', flag='r')
URI_EXCLUDES = set(open('dbpedia_uri_excludes.txt').read().splitlines())
LOWER_INCLUDES = dict([line.strip().split('\t') for line in open('dbpediadb_lower.txt')])


def resolve_entity(words):
    """Recursive entity resolution"""
    for i in range(len(words), 0, -1):
        for j, ngram in enumerate(nltk.ngrams(words, i)):
            ngram_joined = ' '.join(ngram)
            label = ngram_joined.replace(' ', '_')
            if label in LOWER_INCLUDES:
                label = LOWER_INCLUDES[label]
            if label not in URI_EXCLUDES and label in dbpedia_typesdb:
                # check canonical uri
                types = dbpedia_typesdb[label]
                # take only the first type for now!!
                # TODO: to type or not to type. That is the question.
                uri = '<dbpedia:'+types[0]+'>'
                new_words = []
                new_words.extend(resolve_entity(words[:j]))
                new_words.append(uri)
                new_words.extend(resolve_entity(words[j+len(ngram):]))
                return new_words
    return words

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    # consider only 5-grams, then generate low-level
    if len(orig_ngram.split()) < 5:
        continue
    new_words = resolve_entity(orig_ngram.split())
    new_ngram = ' '.join(new_words)

    if new_ngram != orig_ngram:
        for i in range(1,6):
            for ngram in nltk.ngrams(new_words, i):
                ngram = ' '.join(ngram)
                if '<dbpedia:' in ngram:
                    print '%s\t%s' % (ngram.strip(), num)

dbpedia_typesdb.close()
