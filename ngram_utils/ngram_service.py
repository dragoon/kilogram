# coding=utf-8
from __future__ import division

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from .libs.hbase import Hbase
import pymongo


class NgramService(object):

    @classmethod
    def configure(cls, mongo_host, hbase_host):
        cls.m_client = pymongo.MongoClient(host=mongo_host)
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

    @classmethod
    def hbase_count(cls, table, ngram):
        """
        :rtype: int
        """
        cls.h_rate += 1
        time_diff = time.time() - cls.h_start
        if time_diff > 30:
            print "HBase req rate:", cls.h_rate/time_diff, "r/s"
            cls.start = time.time()
            cls.rate = 0
        try:
            res = cls.h_client.get(table, ngram, "ngram:cnt", None)
            return long(res[0].value)
        except (ValueError, IndexError):
            return 0

    @classmethod
    def get_freq(cls, ngram, is_sub):
        """Get ngram frequency from Google Ngram corpus"""
        split_len = len(ngram.split())
        if is_sub:
            if split_len == 2:
                res = cls.m_2grams.find_one({'ngram': ngram})
            elif split_len == 3:
                res = cls.m_3grams.find_one({'ngram': ngram})
            else:
                raise Exception('%d-grams are not supported yet' % split_len)
            try:
                res = dict([((ngram.replace('SUB', subst), count) for subst, count in res['count'].items()]
            except:
                res = {ngram: 0}
        else:
            if split_len == 1:
                try:
                    count = cls.m_1grams.find_one({'ngram': ngram})['count']
                except:
                    count = 0
            elif split_len == 2:
                count = cls.hbase_count('ngrams2', ngram)
            else:
                raise Exception('%d-grams are not supported' % split_len)
            res = {ngram: count}
        return res
