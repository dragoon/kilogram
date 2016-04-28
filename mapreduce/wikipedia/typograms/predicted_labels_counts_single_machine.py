from __future__ import print_function
import sys
import codecs
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive
from kilogram.dataset.edit_histories.wikipedia import line_filter


organic_label_dict = {}

for line in codecs.open(sys.argv[1], 'r', 'utf-8'):
    try:
        label, uri, count = line.strip().split('\t')
    except:
        # sometimes labels are empty
        continue
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

for line in sys.stdin:
    for item in generate_ngrams(line):
        (label, uri), count = item
        print(label + '\t' + uri + '\t' + unicode(count))

