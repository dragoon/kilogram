#!/usr/bin/env python

import sys
import nltk
import anydbm

# Open just for read
dbpediadb = anydbm.open('dbpedia.dbm', 'r')
URI_EXCLUDES = set(open('dbpedia_uri_excludes.txt').read().splitlines())


def resolve_entity(words):
    """Recursive entity resolution"""
    for i in range(len(words), 0, -1):
        for j, ngram in enumerate(nltk.ngrams(words, i)):
            ngram_joined = ' '.join(ngram)
            if ngram_joined in dbpediadb:
                uri = dbpediadb[ngram_joined]
                if uri in URI_EXCLUDES:
                    continue
                new_words = []
                new_words.extend(words[:j])
                new_words.append(uri)
                new_words.extend(resolve_entity(words[j+len(ngram):]))
                return new_words
    return words

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    new_words = resolve_entity(orig_ngram.split())
    new_ngram = ' '.join(new_words)

    if new_ngram != orig_ngram:
        print '%s\t%s' % (new_ngram.strip(), num)

dbpediadb.close()
