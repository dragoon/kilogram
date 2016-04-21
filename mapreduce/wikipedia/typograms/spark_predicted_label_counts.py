"""
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
"""
import sys
import codecs
from pyspark import SparkContext
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive
from kilogram.dataset.edit_histories.wikipedia import line_filter

sc = SparkContext(appName="WikipediaPredictedLabelCounts")

wiki_plain = sc.textFile(sys.argv[2])

organic_label_dict = {}

for line in codecs.open(sys.argv[1], 'r', 'utf-8'):
    label, uri, count = line.split('\t')
    organic_label_dict[label] = uri


def generate_ngrams(line):
    labels = []
    line = line.strip()
    for sentence in line_filter(' '.join(tokenize_possessive(default_tokenize_func(line)))):
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(min(len(sentence), i+20), i, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> skip, could be wrong (Apple)
                    continue
                elif token in organic_label_dict:
                    labels.append(((token, organic_label_dict[token]), 1))
                    i = j-1
                    break
            i += 1
    return labels


wiki_predicted_labels = wiki_plain.flatMap(generate_ngrams).reduceByKey(lambda n1, n2: n1+n2)

def printer(item):
    (label, uri), count = item
    return [label + '\t' + uri + '\t' + unicode(count)]

wiki_predicted_labels.flatMap(printer).saveAsTextFile(sys.argv[3])
