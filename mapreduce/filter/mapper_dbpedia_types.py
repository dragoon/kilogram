#!/usr/bin/env python

import sys
import shelve

# Open just for read
dbpediadb = shelve.open('dbpedia_types.dbm', flag='r')

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')

    words = ngram.split()
    dbpedia_words = [(i, word) for i, word in enumerate(words) if word[0] == '<' and word[-1] == '>']
    dbp_dict = {}
    for i, word in dbpedia_words:
        if word in dbpediadb:
            dbp_dict[i] = dbpediadb[word]
        else:
            words[i:i+1] = words[i][1:-1].split('_')

    if not dbp_dict:
        continue

    ngrams = [words[:]]
    for i, types in dbp_dict.items():
        new_ngrams = []
        for words in ngrams:
            for dbp_type in types:
                new_words = words[:]
                new_words[i] = dbp_type
                new_ngrams.append(new_words)
                # take only the first type for now!!
                # TODO: to type or not to type. That is the question.
                break
        ngrams = new_ngrams

    for new_words in ngrams:
        print '%s\t%s' % (' '.join(new_words), num)

dbpediadb.close()
