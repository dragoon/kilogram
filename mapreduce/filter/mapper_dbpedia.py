#!/usr/bin/env python

import sys
import nltk
import anydbm

# Open just for read
dbpediadb = anydbm.open('dbpedia.dbm', 'r')
URI_EXCLUDES = set(open('dbpedia_uri_excludes.txt').read().splitlines())

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    words = ngram.split()
    for i in range(len(words), 0, -1):
        stop = 0
        for j, ngram in enumerate(nltk.ngrams(words, i)):
            ngram_joined = ' '.join(ngram)
            if ngram_joined in dbpediadb:
                uri = dbpediadb[ngram_joined]
                if uri in URI_EXCLUDES:
                    continue
                stop = True
                new_words = []
                new_words.extend(words[:j])
                new_words.append(uri)
                new_words.extend(words[j+len(ngram):])
                new_ngram = ' '.join(new_words)
                print '%s\t%s' % (new_ngram.strip(), num)
        if stop:
            break

dbpediadb.close()