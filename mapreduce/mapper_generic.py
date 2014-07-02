#!/usr/bin/env python

import sys
import re
import string
from zipfile import ZipFile

from kilogram.lang import number_replace

MY_PRINTABLE = set(string.letters+string.digits+string.punctuation+' ')
MULTI_PUNCT_RE = re.compile(r'(^| )\W+ \W+($| )')

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
            CITIES.add(name)
            CITIES.add(asciiname)
            for name in alternatenames.split(','):
                CITIES.add(name)


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    orig_ngram, num = line.split('\t')
    if not MY_PRINTABLE.issuperset(orig_ngram):
        continue
    # skip POS tags
    is_pos = [1 for word in orig_ngram.split() if '_' in word and word != '_']
    if is_pos:
        continue

    if MULTI_PUNCT_RE.search(orig_ngram):
        continue

    # replace apostrophes without duplicating
    if "'" in orig_ngram:
        orig_ngram = orig_ngram.replace(" '", "'")
        orig_ngram = orig_ngram.replace("' ", "'")
    # percentages as well
    orig_ngram = orig_ngram.replace(" %", "%")

    ngrams = set()

    #-----PERSON ENTITIES--
    new_words = []
    for word in orig_ngram.split():
        if word in NAME_SET:
            new_words.append('PERSON')
        else:
            new_words.append(word)
    new_ngram = ' '.join(new_words)
    if new_ngram != orig_ngram:
        ngrams.add(new_ngram)
    #-----END-------------

    #---GEO ENTITIES-------
    new_words = []
    for word in orig_ngram.split():
        if word in CITIES:
            new_words.append('CITY')
        else:
            new_words.append(word)
    new_ngram = ' '.join(new_words)
    if new_ngram != orig_ngram:
        ngrams.add(new_ngram)
    #-----END-------------

    #--DBPEDIA ENTITIES-------
    #--END--------------------

    ngrams.add(orig_ngram.lower())
    if '-' in orig_ngram:
        for ngram in ngrams:
            ngram = ngram.replace(' -', '-')
            ngram = ngram.replace('- ', '-')
            ngrams.add(ngram)

    for ngram in ngrams:
        words = ngram.split()
        new_words = []
        for word in words:
            # numeric entities, DBPEDIA entities can contain numbers, so replace last
            new_words.append(number_replace(word))

        print '%s\t%s' % (' '.join(new_words), num)
