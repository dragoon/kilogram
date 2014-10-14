# coding=utf-8
from __future__ import division
import time
from collections import defaultdict

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from .hbase import Hbase
from . import DEBUG
import pymongo

SUBSTITUTION_TOKEN = 'SUB'


class NgramService(object):
    h_rate = None
    h_start = None
    h_client = None

    @staticmethod
    def _is_subst(ngram):
        return SUBSTITUTION_TOKEN in set(ngram)

    @classmethod
    def configure(cls, substitutions, mongo_host=('localhost', '27017'), hbase_host=('localhost', '9090')):
        cls.substitutions = sorted(substitutions)

        cls.m_client = pymongo.MongoClient(host=mongo_host[0], port=int(mongo_host[1]))
        cls.m_1grams = cls.m_client['1grams']['default']
        cls.m_2grams = cls.m_client['2grams']['default']
        cls.m_3grams = cls.m_client['3grams']['default']

        # HBASE
        cls.h_transport = TTransport.TBufferedTransport(TSocket.TSocket(*hbase_host))
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(cls.h_transport)
        cls.h_client = Hbase.Client(protocol)
        cls.h_transport.open()
        cls.h_rate = 0
        cls.h_start = time.time()

        cls.substitution_counts = dict([(subst, cls.get_freq(subst)[subst]) for subst in cls.substitutions])

    @classmethod
    def hbase_count(cls, table, ngram):
        """
        :rtype: int
        """
        cls.h_rate += 1
        time_diff = time.time() - cls.h_start
        if time_diff > 30 and DEBUG:
            print "HBase req rate:", cls.h_rate/time_diff, "r/s"
            cls.h_start = time.time()
            cls.h_rate = 0
        try:
            res = cls.h_client.get(table, ngram, "ngram:cnt", None)
            return long(res[0].value)
        except (ValueError, IndexError):
            return 0

    @classmethod
    def get_freq(cls, ngram):
        """Get ngram frequency from Google Ngram corpus"""
        split_ngram = ngram.split()
        split_len = len(split_ngram)
        if NgramService._is_subst(split_ngram):
            sub_index = split_ngram.index(SUBSTITUTION_TOKEN)
            if split_len == 1:
                return cls.substitution_counts
            if split_len == 2:
                res = cls.m_2grams.find_one({'ngram': ngram})
            elif split_len == 3:
                res = cls.m_3grams.find_one({'ngram': ngram})
            else:
                raise Exception('%d-grams are not supported yet' % split_len)
            try:
                counts = res['count']
            except:
                counts = defaultdict(lambda: 0)
            res = {}
            for subst in cls.substitutions:
                cur_ngram = split_ngram[:]
                cur_ngram[sub_index] = subst
                res[tuple(cur_ngram)] = long(counts.get(subst, 0))
        else:
            if split_len == 1:
                try:
                    count = cls.m_1grams.find_one({'ngram': ngram})['count']
                except:
                    count = 0
                # Stupid NLTK needs a word when 1gram, and tuple if n>1
                res = {ngram: count}
            elif split_len == 2:
                count = cls.hbase_count('ngrams2', ngram)
                res = {tuple(split_ngram): count}
            else:
                raise Exception('%d-grams are not supported' % split_len)
        return res
