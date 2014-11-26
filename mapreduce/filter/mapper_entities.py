#!/usr/bin/env python

import sys
from zipfile import ZipFile

import nltk

# NAMES corpus
from nltk.corpus import names
NAME_SET = set()
for f in names.fileids():
    NAME_SET = NAME_SET.union(names.words(f))

# wget http://download.geonames.org/export/dump/cities15000.zip
GEONAMES_FILE = 'cities15000.zip'

# Prepare geonames
CITIES = set()
with ZipFile(GEONAMES_FILE) as zip_file:
    for filename in zip_file.namelist():
        contents = zip_file.open(filename)
        for line in contents:
            geonameid, name, asciiname, alternatenames, other = line.split('\t', 4)
            other = other.split('\t')
            population = int(other[-5])
            if population < 100000:
                continue
            CITIES.add(tuple(name.split()))
            CITIES.add(tuple(asciiname.split()))
            for name in alternatenames.split(','):
                CITIES.add(tuple(name.split()))


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')

    ngrams = set()

    #-----PERSON ENTITIES--
    new_words = []
    for word in orig_ngram.split():
        if word in NAME_SET:
            new_words.append('<PERSON>')
        else:
            new_words.append(word)
    new_ngram = ' '.join(new_words)
    if new_ngram != orig_ngram:
        ngrams.add(new_ngram)
    #-----END-------------

    #---GEO ENTITIES-------
    cur_ngrams = list(ngrams)
    cur_ngrams.append(orig_ngram)
    for ngram1 in cur_ngrams:
        words = ngram1.split()
        for i in range(len(words), 0, -1):
            stop = 0
            for j, ngram in enumerate(nltk.ngrams(words, i)):
                if ngram in CITIES:
                    stop = 1
                    new_words = []
                    new_words.extend(words[:j])
                    new_words.append('<CITY>')
                    new_words.extend(words[j+len(ngram):])
                    new_ngram = ' '.join(new_words)
                    ngrams.add(new_ngram.strip())
            if stop:
                break
    #-----END-------------

    for ngram in ngrams:
        print '%s\t%s' % (' '.join(new_words), num)
