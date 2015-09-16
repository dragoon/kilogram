"""
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/spark_typed_ngrams.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_typed_ngrams" 3
"""
import sys
from pyspark import SparkContext
import re
import nltk
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.dataset.wikipedia import line_filter

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')

N = int(sys.argv[3])


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
        tokens_types = []
        for word in sentence.split():
            match = ENTITY_MATCH_RE.match(word)
            if match:
                uri = match.group(1)
                tokens_types.append('<wiki:'+uri+'>')
            else:
                tokens_types.append(word)
        # do not split title-case sequences
        tokens_types = merge_titlecases(tokens_types)
        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_types, n):
                type_indexes = [i for i, x in enumerate(ngram) if x.startswith('<wiki:')]
                if len(type_indexes) == 0:
                    continue
                type_index = type_indexes[0]
                result.append((ngram[type_index][9:-1], (type_index, ngram)))
    return result


def map_ngrams(ngram_count):
    ngram, count = ngram_count
    type_indexes = [i for i, x in enumerate(ngram) if x.startswith('<wiki:')]
    if len(type_indexes) == 0:
        return []
    type_index = type_indexes[0]
    return [(ngram[type_index][9:-1], (type_index, ngram))]


def map_type_ngram(ngram_tuple):
    entity_type = ngram_tuple[1][1]
    type_index, ngram = ngram_tuple[1][0]
    ngram = list(ngram)
    ngram[type_index] = '<dbpedia:' + entity_type + '>'
    return tuple(ngram), 1

dbp_types_file = sc.textFile("/user/roman/dbpedia_types.txt")
dbp_labels = dbp_types_file.map(lambda uri_types: uri_types.strip().split('\t'))

ngrams = lines.flatMap(generate_ngrams).join(dbp_labels)
typed_ngrams = ngrams.map(map_type_ngram).reduceByKey(lambda n1, n2: n1 + n2)

def printer(value):
    return ' '.join(value[0]) + '\t' + str(value[1])

for i in range(N):
    typed_ngrams.filter(lambda x: x[1] > 1 and not any(y for y in x[0] if y.startswith('<wiki:'))).map(printer).saveAsTextFile(sys.argv[2]+str(i))
    typed_ngram = typed_ngrams.flatMap(map_ngrams).join(dbp_labels).map(map_type_ngram).reduceByKey(lambda n1, n2: n1 + n2)
