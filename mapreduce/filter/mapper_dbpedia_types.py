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
    dbpedia_words = [(i, word) for i, word in enumerate(words) if word[:9] == '<dbpedia:' and word[-1] == '>']
    dbp_dict = {}
    to_replace_index = []
    for i, word in dbpedia_words:
        word = word[9:-1]
        if word in dbpediadb:
            dbp_dict[i] = dbpediadb[word]
        else:
            # always insert in the beginning to later iterate from the highest position
            to_replace_index.insert(0, i)

    if not dbp_dict:
        continue

    ngrams = [words[:]]
    for i, types in dbp_dict.items():
        new_ngrams = []
        for words in ngrams:
            for dbp_type in types:
                new_words = words[:]
                new_words[i] = '<dbpedia:'+dbp_type+'>'
                new_ngrams.append(new_words)
                # take only the first type for now!!
                # TODO: to type or not to type. That is the question.
                break
        ngrams = new_ngrams

    # Replace after to not alter the positions
    for i in to_replace_index:
        for new_words in ngrams:
            new_words[i:i+1] = new_words[i][1:-1].split('_')

    for new_words in ngrams:
        print '%s\t%s' % (' '.join(new_words), num)

dbpediadb.close()
