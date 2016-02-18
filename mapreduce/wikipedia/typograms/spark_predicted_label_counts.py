"""
spark-submit --executor-memory 3g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
"""
import sys
from pyspark import SparkContext
from kilogram.lang.tokenize import default_tokenize_func
from kilogram.dataset.edit_histories.wikipedia import line_filter

sc = SparkContext(appName="WikipediaPredictedLabelCounts")

wiki_plain = sc.textFile(sys.argv[1])

organic_label_dict = {}

for line in open("organic_label_counts.txt"):
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
            for j in range(len(sentence), -1, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> skip, could be wrong (Apple)
                    continue
                elif token in organic_label_dict:
                    labels.append((token.lower(), organic_label_dict[token], token, 1))
                    i = j-1
                    break
            i += 1
    return labels

wiki_predicted_labels = wiki_plain.flatMap(generate_ngrams)
wiki_predicted_labels.saveAsTextFile(sys.argv[2])
