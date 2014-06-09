#!/usr/bin/env python
"""
Inserts ngrams for tab-separated file into specified mongoDB collection,
Available databases: unigrams, bigrams, trigrams.
Available types: generic, subs
"""
import codecs
import argparse
import pymongo

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-h', '--host', dest='host', action='store', default='localhost',
                   help='mongodb host')
parser.add_argument('-d', '--database', dest='db', action='store', required=True,
                   help='database name')
parser.add_argument('--subs', dest='subs', action='store_true',
                   help='file contains substitutions')
parser.add_argument('files', nargs='+', help='ngram/count tab-separated files')

args = parser.parse_args()
MONGO_CLIENT = pymongo.MongoClient(host=args.host)
DB = MONGO_CLIENT[args.db]
COLLECTION = DB['default']


def _insert_preps(tsv_file):
    i = 0
    j = 0
    temp_list = []
    subs = {}
    cur_ngram_pos = None
    for line in tsv_file:
        ngram_pos, subst, count = line.strip().split('\t')
        if cur_ngram_pos == ngram_pos:
            subs[subst] = count
        else:
            if cur_ngram_pos is not None:
                cur_ngram, cur_pos = cur_ngram_pos.rsplit(' ', 1)
                temp_list.append({'ngram': cur_ngram, 'count': subs})
                preps = {}
            subs[subst] = count
            cur_ngram_pos = ngram_pos
            if i - 200000 > 0:
                j += i
                i = 0
                self.col.insert(temp_list)
                temp_list = []
                print 'Inserted: ', j
        i += 1

    # append whatever left, continue if duplicate id found
    COLLECTION.insert(temp_list)
    print 'Inserted', j+i

def _insert_all(tsv_file):
    i = 0
    temp_list = []
    for line in tsv_file:
        ngram, count = line.strip().split('\t')
        temp_list.append({'ngram': ngram, 'count': int(count)})
        if i != 0 and i % 100000 == 0:
            self.col.insert(temp_list)
            temp_list = []
            print 'Inserted: ', i
        i += 1
    COLLECTION.insert(temp_list)
    print 'Inserted', i

if args.subs:
    insert_func = self._insert_preps
else:
    insert_func = self._insert_all

for tsv_file in args.files:
    print 'Processing file {0}...'.format(tsv_file)
    tsv_file = codecs.open(tsv_file, 'r', 'utf-8')
    insert_func(tsv_file)

COLLECTION.ensure_index([('ngram', 1)])
