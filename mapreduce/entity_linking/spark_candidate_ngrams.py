"""
spark-submit --master yarn-client ./entity_linking/spark_candidate_ngrams.py "/user/roman/dbpedia_types.txt" "/user/roman/wikipedia_anchors" "/user/roman/candidate_ngram_links"
pig -p table=wiki_anchor_ngrams -p path=/user/roman/wikipedia_anchors ./hbase_upload_array.pig
pig -p table=wiki_anchor_ngrams -p path=/user/roman/candidate_ngram_links ./hbase_upload_array.pig
"""
import sys
from pyspark import SparkContext
import re
from itertools import combinations
from kilogram import ListPacker


LABEL_RE = re.compile(r'(.+)_\(.+\)')


sc = SparkContext(appName="CandidateEntityLinkings")


def generate_label_ngrams(uri):
    result = []
    match = LABEL_RE.match(uri)
    label = uri
    if match:
        label = match.group(1)
    label = label.replace('_', ' ').split()
    for i in range(1, len(label)+1):
        for ngram in combinations(label, i):
            result.append(' '.join(ngram))
    return uri, result


types_file = sys.argv[1]
labels = sc.textFile(types_file).filter(lambda line: '__' not in line)\
    .map(lambda x: x.split('\t')[0]).distinct().map(generate_label_ngrams)


def map_uris(anchor_line):
    result = []
    _, uris = anchor_line.split('\t')
    for uri, v in ListPacker.unpack(uris):
        result.append((uri, long(v)))
    return result

anchors_file = sys.argv[2]
anchor_counts = sc.textFile(anchors_file).flatMap(map_uris).reduceByKey(lambda x, y: x+y)

join = labels.leftOuterJoin(anchor_counts)


def printer(value):
    return value[0] + '\t' + value[1]

join.saveAsTextFile(sys.argv[3])
