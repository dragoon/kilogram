import sys
import nltk
import codecs
from pyspark import SparkContext
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.lang.tokenize import default_tokenize_func
from kilogram.dataset.edit_histories.wikipedia import line_filter

N = int(sys.argv[3])

sc = SparkContext(appName="SparkGenerateTypedNgrams")

lines = sc.textFile(sys.argv[1])

unambiguous_labels = {}

for line in codecs.open("unambiguous_labels.txt", 'r', 'utf-8'):
    label, uri = line.split('\t')
    unambiguous_labels[label] = uri


# Split each line into words
def generate_ngrams(line):
    result = []
    line = line.strip()
    for sentence in line_filter(' '.join(default_tokenize_func(line))):
        tokens_plain = []
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(min(len(sentence), i+20), i, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> do not attempt to link, could be wrong (Apple)
                    tokens_plain.append(token)
                elif token in unambiguous_labels:
                    uri = unambiguous_labels[token]
                    # get types
                    tokens_plain.append('<dbpedia:'+uri+'>')
                    i = j-1
                    break
            i += 1
        for n in range(1, N+1):
            for ngram in nltk.ngrams(tokens_plain, n):
                result.append((' '.join(ngram), 1))
    return result

ngrams = lines.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1 + n2).filter(lambda x: x[1] > 1)

#ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_2015-04.owl")
# free some space
#ner.redirects_file = None


def printer(value):
    return value[0] + '\t' + str(value[1])

ngrams.map(printer).saveAsTextFile(sys.argv[2])
