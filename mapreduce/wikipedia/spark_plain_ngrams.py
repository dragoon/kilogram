"""
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/spark_plain_ngrams.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_ngrams"
"""
import sys
from pyspark import SparkContext
import re
import os
import nltk
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')

N = int(os.environ['NGRAM'])
if not N:
    print 'N is not specified'
    exit(0)


def merge_titlecases(tokens):
    new_tokens = []
    last_title = False
    for token in tokens:
        if token[0].isupper():
            if last_title:
                new_tokens[-1] += ' ' + token
            else:
                new_tokens.append(token)
            last_title = True
        else:
            new_tokens.append(token)
            last_title = False
    return new_tokens


sc = SparkContext(appName="SparkGenerateNgrams")

lines = sc.textFile(sys.argv[1])

# Split each line into words
def generate_ngrams(line):
    def partition(alist, indices):
        return [alist[i+1:j] for i, j in zip([-1]+indices, indices+[None])]
    result = []
    line = line.strip()
    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        tokens_types, tokens_plain = parse_types_text(sentence, {})

        # do not split title-case sequences
        tokens_plain = merge_titlecases(tokens_plain)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_plain, n):
                result.append((' '.join(ngram), 1))
    return result

ngrams = lines.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x: x[1] > 1)

def printer(value):
    return value[0] + '\t' + str(value[1])

ngrams.map(printer).saveAsTextFile(sys.argv[2])
