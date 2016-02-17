"""
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/spark_plain_ngrams.py "/user/roman/dbpedia_data.txt" "/data/wikipedia2015_plaintext" "/user/roman/wikipedia_ngrams" 3
"""
import sys

import nltk

from pyspark import SparkContext
from kilogram.lang.tokenize import default_tokenize_func
from kilogram.dataset.edit_histories.wikipedia import line_filter

N = int(sys.argv[4])


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

dbpedia_data = sc.textFile(sys.argv[1])
lines = sc.textFile(sys.argv[2])


# Split each line into words
def generate_ngrams(line):
    result = []
    line = line.strip()
    for sentence in line_filter(' '.join(default_tokenize_func(line))):
        tokens_plain = []
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(len(sentence), -1, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j:
                    tokens_plain.append(token)
                elif token in dbpedia_data:
                    tokens_plain.append(token)
                    i = j-1
                    break
            i += 1
        # do not split title-case sequences
        tokens_plain = merge_titlecases(tokens_plain)
        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_plain, n):
                result.append((' '.join(ngram), 1))
    return result

ngrams = lines.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x: x[1] > 1)


def printer(value):
    return value[0] + '\t' + str(value[1])

ngrams.map(printer).saveAsTextFile(sys.argv[3])
