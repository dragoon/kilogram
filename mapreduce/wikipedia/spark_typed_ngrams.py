"""
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/spark_typed_ngrams.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_typed_ngrams" 3
"""
import sys
from pyspark import SparkContext
import re
import nltk
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.dataset.wikipedia import line_filter
from kilogram.dataset.wikipedia.entities import parse_types_text

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')

N = sys.argv[3]


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


sc = SparkContext(appName="SparkGenerateTypedNgrams")

lines = sc.textFile(sys.argv[1])

# generate tuples of form (URI, (uri_index, ngram_tuple))
def generate_ngrams(line):
    result = []
    line = line.strip()
    for sentence in line_filter(' '.join(wiki_tokenize_func(line))):
        tokens_types, _ = parse_types_text(sentence, {})

        # do not split title-case sequences
        tokens_types = merge_titlecases(tokens_types)

        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_types, n):
                type_indexes = [i for i, x in enumerate(ngram) if ENTITY_MATCH_RE.match(x)]
                if len(type_indexes) == 0:
                    continue
                type_index = type_indexes[0]
                result.append((ngram[type_index], (type_index, ngram)))
    return result

def map_type_ngram(ngram_tuple):
    entity_type = ngram_tuple[1][1]
    type_index, ngram = ngram_tuple[1][0]
    ngram = list(ngram)
    ngram[type_index] = entity_type
    return ' '.join(ngram), 1

dbp_types_file = sc.textFile("/user/roman/dbpedia_types.txt")
dbp_labels = dbp_types_file.map(lambda uri_types: uri_types.strip().split('\t'))

ngrams = lines.flatMap(generate_ngrams).join(dbp_labels)
typed_ngrams = ngrams.map(map_type_ngram).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x: x[1] > 1)

def printer(value):
    return value[0] + '\t' + str(value[1])

typed_ngrams.map(printer).saveAsTextFile(sys.argv[2])
