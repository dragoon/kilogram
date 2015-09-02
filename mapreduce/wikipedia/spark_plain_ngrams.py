"""
spark-submit --num-executors 20 --master yarn-client ./wikipedia/spark_plain_ngrams.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_ngrams"
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
        for i in range(1, 6):
            for ngram in nltk.ngrams(sublist, i):
                result.append((' '.join(ngram), 1))
    return result

ngrams = lines.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x, y: y > 1)

def printer(value):
    return value[0] + '\t' + value[1]

ngrams.map(printer).saveAsTextFile(sys.argv[2])
