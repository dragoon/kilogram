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
        :type list_counts: list of tuples (str, int) or (str, str)
        :return: str
        """
        return ' '.join([x[0]+','+str(x[1]) for x in list_counts])

    @classmethod
    def unpack(cls, list_counts_str):
        """
        :return: list of tuples
        """
        if list_counts_str:
            return [x.rsplit(',', 1) for x in list_counts_str.split()]
        else:
            return []


class NgramService(object):
    h_rate = None
    h_start = None
    h_client = None
    substitutions = None
    substitution_counts = None
    subst_table = None
    ngram_table = None
    wiki_anchors_table = None
    wiki_urls_table = None
    wiki_pagelinks_table = None

    @staticmethod
    def _is_subst(ngram):
        return SUBSTITUTION_TOKEN in set(ngram)

    @classmethod
    def configure(cls, ngram_table="ngrams", subst_table="ngram_types",
                  wiki_anchors_table="wiki_anchors", wiki_urls_table="wiki_urls",
                  wiki_pagelinks_table="wiki_pagelinks", hbase_host=None):
        cls.subst_table = subst_table
        cls.ngram_table = ngram_table
        cls.wiki_urls_table = wiki_urls_table
        cls.wiki_anchors_table = wiki_anchors_table
        cls.wiki_pagelinks_table = wiki_pagelinks_table

        # HBASE
        cls.h_transport = TTransport.TBufferedTransport(TSocket.TSocket(*hbase_host))
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(cls.h_transport)
        cls.h_client = Hbase.Client(protocol)
        cls.h_transport.open()
        cls.h_rate = 0
        cls.h_start = time.time()

        cls.substitution_counts = cls.get_freq(SUBSTITUTION_TOKEN)
        cls.substitutions = sorted(cls.substitution_counts.keys())

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
                if cls.substitution_counts:
                    return cls.substitution_counts
                else:
                    counts = ListPacker.unpack(NgramService.hbase_raw(cls.subst_table, SUBSTITUTION_TOKEN, "ngram:value"))
                    return dict((word, long(count)) for word, count in counts)
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

    @classmethod
    def get_wiki_prob(cls, phrase):
        """Get wiki probability of a phrase"""
        page_counts = ListPacker.unpack(NgramService.hbase_raw(cls.wiki_anchors_table, phrase, "ngram:value"))
        anchor_counts = sum([long(x[1]) for x in page_counts])
        # add 10 to compensate for small counts
        wiki_counts = sum([cls.hbase_count(cls.wiki_urls_table, x[0].decode('utf-8')) for x in page_counts]) + 10
        if wiki_counts -10 < anchor_counts:
            print "PROBABILITY ERROR"
        return anchor_counts/wiki_counts

    @classmethod
    def get_ref_count(cls, uri, test_uri_list):
        test_uri_set = set(test_uri_list)
        uri_counts = ListPacker.unpack(NgramService.hbase_raw(cls.wiki_pagelinks_table, uri, "ngram:value"))
        uri_counts = [x for x in uri_counts if x[0] in test_uri_set]
        if uri_counts:
            high = max(uri_counts, key=lambda x: int(x[1]))[1]
            return [x[0] for x in uri_counts if x[1]==high]
        if uri in test_uri_set:
            return [uri]
        return None
