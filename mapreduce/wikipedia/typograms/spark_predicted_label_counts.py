"""
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
"""
import sys
import codecs
from pyspark import SparkContext
from kilogram.lang.tokenize import default_tokenize_func
from kilogram.dataset.edit_histories.wikipedia import line_filter

sc = SparkContext(appName="WikipediaPredictedLabelCounts")

wiki_plain = sc.textFile(sys.argv[1])

organic_label_dict = {}

for line in codecs.open("organic_label_counts.txt", 'r', 'utf-8'):
    uri, label, counts = line.split('\t')
    if not label.islower():
        organic_label_dict[label.lower()] = uri
    organic_label_dict[label] = uri


def generate_ngrams(line):
    labels = []
    line = line.strip()
    for sentence in line_filter(' '.join(default_tokenize_func(line))):
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(min(len(sentence), i+20), i, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> skip, could be wrong (Apple)
                    continue
                elif token in organic_label_dict:
                    labels.append(((token.lower(), organic_label_dict[token]), {token: 1}))
                    i = j-1
                    break
            i += 1
    return labels

def collect_tokens(value1, value2):
    for label, count in value1.items():
        if label in value2:
            value2[label] += count
        else:
            value2[label] = count
    return value2

wiki_predicted_labels = wiki_plain.flatMap(generate_ngrams).reduceByKey(collect_tokens)

def printer(item):
    uri = item[0][1]
    label_lower = item[0][0]
    count_dict = item[1]
    count_lower = '0'
    count_normal = '0'
    if label_lower in count_dict:
        count_lower = count_dict[label_lower]
        del count_dict[label_lower]
    if len(count_dict) > 0:
        label_lower, count_normal = count_dict.items()[0]
    return uri + '\t' + label_lower + '\t' + unicode(count_normal)+','+unicode(count_lower)

wiki_predicted_labels.map(printer).saveAsTextFile(sys.argv[2])
