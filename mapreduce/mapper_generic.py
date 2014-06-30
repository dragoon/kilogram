#!/usr/bin/env python

import sys
import string
from zipfile import ZipFile

from kilogram.lang import number_replace

MY_PRINTABLE = set(string.letters+string.digits+string.punctuation+' ')

# wget http://download.geonames.org/export/dump/cities15000.zip
GEONAMES_FILE = '/home/roman/cities15000.zip'

# Prepare geonames
CITIES = set()
with ZipFile(GEONAMES_FILE) as zip_file:
    for filename in zip_file.namelist():
        contents = zip_file.open(filename)
        for line in contents:
            geonameid, name, asciiname, alternatenames, _ = line.split('\t', 4)
            CITIES.add(name)
            CITIES.add(asciiname)
            for name in alternatenames.split(','):
                CITIES.add(name)


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    ngram, num = line.split('\t')
    if not MY_PRINTABLE.issuperset(ngram):
        continue
    # skip POS tags
    is_pos = [1 for word in ngram.split() if '_' in word and word != '_']
    if is_pos:
        continue

    # replace apostrophes without duplicating
    if "'" in ngram:
        ngram = ngram.replace(" '", "'")
        ngram = ngram.replace("' ", "'")
    # percentages as well
    ngram = ngram.replace(" %", "%")

    ngrams = {ngram}
    if '-' in ngram:
        ngram = ngram.replace(' -', '-')
        ngram = ngram.replace('- ', '-')
        ngrams.add(ngram)

    for ngram in ngrams:
        words = ngram.split()
        new_words = []
        for word in words:
            # numeric entities
            new_words.append(number_replace(word))
        ngram = ' '.join(new_words)

        print '%s\t%s' % (ngram, num)
