#!/usr/bin/env python
"""
Inserts ngrams for tab-separated file into specified mongoDB collection,
Available databases: unigrams, bigrams, trigrams.
Available types: generic, subs
"""
import codecs
import argparse


class Command(BaseCommand):
    args = 'ngram/count tab-separated file'

    option_list = BaseCommand.option_list + (
        make_option('--collection', '-c',
                    action='store',
                    dest='col',
                    help='collection name'),
        make_option('--database', '-d',
                    action='store',
                    dest='db',
                    help='database name'),
    )

    def _insert_preps(self, tsv_file):
        i = 0
        j = 0
        temp_list = []
        preps = {}
        cur_ngram_pos = None
        for line in tsv_file:
            ngram_pos, prep, count = line.strip().split('\t')
            if cur_ngram_pos == ngram_pos:
                preps[prep] = count
            else:
                if cur_ngram_pos is not None:
                    cur_ngram, cur_pos = cur_ngram_pos.rsplit(' ', 1)
                    temp_list.append({'ngram': cur_ngram, 'position': int(cur_pos), 'preps': preps})
                    preps = {}
                preps[prep] = count
                cur_ngram_pos = ngram_pos
                if i - 200000 > 0:
                    j += i
                    i = 0
                    self.col.insert(temp_list)
                    temp_list = []
                    print 'Inserted: ', j
            i += 1

        # append whatever left, continue if duplicate id found
        self.col.insert(temp_list)
        print 'Inserted', j+i

    def _insert_all(self, tsv_file):
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
        self.col.insert(temp_list)
        print 'Inserted', i

    def handle(self, *args, **options):
        db = options['db']
        col_name = options['col']
        if not db:
            raise CommandError("need to specify database name")
        if not col_name:
            raise CommandError("need to specify collection name")
        db = settings.MONGO_CLIENT[db]
        self.col = db[col_name]
        if col_name.startswith('preps'):
            insert_func = self._insert_preps
        else:
            insert_func = self._insert_all

        for tsv_file in args:
            print 'Processing file {0}...'.format(tsv_file)
            tsv_file = codecs.open(tsv_file, 'r', 'utf-8')
            insert_func(tsv_file)

        self.col.ensure_index([('ngram', 1)])
