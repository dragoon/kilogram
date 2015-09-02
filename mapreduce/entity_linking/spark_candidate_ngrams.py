"""
spark-submit --master yarn-client ./entity_linking/candidate_ngrams_spark.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_anchors_orig" "/user/roman/candidate_ngram_links"
"""
import sys
from pyspark import SparkContext
import re
import nltk

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')


sc = SparkContext(appName="CandidateEntityLinkings")

lines = sc.textFile(sys.argv[1])

# Split each line into words
def generate_ngrams(line):
    def partition(alist, indices):
        return [alist[i+1:j] for i, j in zip([-1]+indices, indices+[None])]
    result = []
    line = line.strip().split()
    entity_indexes = [i for i, word in enumerate(line) if ENTITY_MATCH_RE.search(word)]
    for sublist in partition(line, entity_indexes):
        for i in range(1, 2):
            for ngram in nltk.ngrams(sublist, i):
                result.append((' '.join(ngram), 1))
    return result


ngrams = lines.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x: x[1] > 1)

def generate_anchor_ngrams(line):
    result = []
    anchors, uris = line.split('\t')
    for i in range(1, 6):
        for ngram in nltk.ngrams(anchors.split(), i):
            result.append((' '.join(ngram), uris))
    return result


anchors = sc.textFile(sys.argv[2]).flatMap(generate_anchor_ngrams)
anchors_join = anchors.join(ngrams)
def printer(value):
    return value[0] + '\t' + value[1][0] + ' NULL,'+str(value[1][1])

anchors_join.map(printer).saveAsTextFile(sys.argv[3])
