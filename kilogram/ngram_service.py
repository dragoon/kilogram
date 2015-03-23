# coding=utf-8
from __future__ import division
import time

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from .hbase import Hbase

SUBSTITUTION_TOKEN = 'SUB'


class ListPacker(object):

    @classmethod
    def pack(cls, list_counts):
        """
        :type list_counts: list of tuples
        :return: str
        """
        return ' '.join([x[0]+','+x[1] for x in list_counts])

    @classmethod
    def unpack(cls, list_counts_str):
        """
        :return: list of tuples
        """
        return [x.split(',') for x in list_counts_str.split()]


class NgramService(object):
    h_rate = None
    h_start = None
    h_client = None
    substitutions = None
    substitution_counts = None
    subst_table = None
    ngram_table = None

    @staticmethod
    def _is_subst(ngram):
        return SUBSTITUTION_TOKEN in set(ngram)

    @classmethod
    def configure(cls, substitutions, ngram_table="ngrams", subst_table="ngram_types", hbase_host=None):
        cls.substitutions = sorted(substitutions)
        cls.subst_table = subst_table
        cls.ngram_table = ngram_table

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
        res = cls.hbase_raw(table, ngram, "ngram:value")
        if res is None:
            res = 0
        else:
            res = long(res)
        return res

    @classmethod
    def hbase_raw(cls, table, ngram, column):
        from . import DEBUG
        cls.h_rate += 1
        time_diff = time.time() - cls.h_start
        if time_diff > 30 and DEBUG:
            print "HBase req rate:", cls.h_rate/time_diff, "r/s"
            cls.h_start = time.time()
            cls.h_rate = 0
        try:
            res = cls.h_client.get(table, ngram.encode('utf-8'), column, None)
            return res[0].value
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _tuple(ngram):
        """
        :type ngram: list
        :returns: tuple with replacement if necessary
        """
        return tuple([x for x in ngram])

    @classmethod
    def get_freq(cls, ngram):
        """Get ngram frequency from Google Ngram corpus"""
        split_ngram = ngram.split()
        split_len = len(split_ngram)
        if NgramService._is_subst(split_ngram):
            sub_index = split_ngram.index(SUBSTITUTION_TOKEN)
            if split_len == 1:
                return cls.substitution_counts
            if 1 < split_len < 5:
                counts = dict(ListPacker.unpack(NgramService.hbase_raw(cls.subst_table, ngram, "ngram:value")))
            else:
                raise Exception('%d-grams are not supported yet' % split_len)
            res = {}
            for subst in cls.substitutions:
                cur_ngram = split_ngram[:]
                cur_ngram[sub_index] = subst
                res[cls._tuple(cur_ngram)] = long(counts.get(subst, 0))
        else:
            count = cls.hbase_count(cls.ngram_table, ngram)
            if split_len == 1:
                res = {ngram: count}
            elif 2 <= split_len <= 3:
                res = {cls._tuple(split_ngram): count}
            else:
                raise Exception('%d-grams are not supported' % split_len)
        return res
