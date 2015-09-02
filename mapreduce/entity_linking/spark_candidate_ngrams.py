"""
spark-submit --master yarn-client ./entity_linking/candidate_ngrams_spark.py "/user/roman/wikipedia_anchors_orig" "/user/roman/candidate_ngram_links"
"""
from collections import defaultdict
import sys
from pyspark import SparkContext
import re
import nltk
from kilogram import ListPacker

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')


sc = SparkContext(appName="CandidateEntityLinkings")


def generate_anchor_ngrams(line):
    result = []
    anchor, uris = line.split('\t')
    for i in range(1, len(anchor)+1):
        for ngram in nltk.ngrams(anchor.split(), i):
            result.append((' '.join(ngram), uris))
    return result


def reduce_anchors(v1, v2):
    res = defaultdict(lambda: 0)
    for k, v in ListPacker.unpack(v1):
        res[k] += long(v)
    for k, v in ListPacker.unpack(v2):
        res[k] += long(v)
    return ListPacker.pack(res.items())


anchors = sc.textFile(sys.argv[1]).flatMap(generate_anchor_ngrams).reduceByKey(reduce_anchors)


def printer(value):
    return value[0] + '\t' + value[1]

anchors.map(printer).saveAsTextFile(sys.argv[2])
