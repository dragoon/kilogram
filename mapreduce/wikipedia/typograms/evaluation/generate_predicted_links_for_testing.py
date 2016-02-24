import sys
import re
import codecs
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive
from kilogram.dataset.edit_histories.wikipedia import line_filter


unambiguous_labels = {}

for line in codecs.open("unambiguous_labels.txt", 'r', 'utf-8'):
    label, uri = line.strip().split('\t')
    unambiguous_labels[label] = uri


ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>')

# Split each line into words
def generate_ngrams(line):
    line = line.strip()
    line = ENTITY_MATCH_RE.sub('\g<2>', line).replace('_', ' ')
    for sentence in line_filter(' '.join(tokenize_possessive(default_tokenize_func(line)))):
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(min(len(sentence), i+20), i, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> do not attempt to link, could be wrong (Apple)
                    continue
                elif token in unambiguous_labels:
                    # check token doesn't span titles
                    if j + 1 < len(sentence) and sentence[j][0].isupper():
                        pass
                    elif i > 0 and sentence[i-1][0].isupper():
                        pass
                    else:
                        uri = unambiguous_labels[token]
                        if token.endswith("'s"):
                            token = token[:-3]
                        # get types
                        print(token.encode('utf-8') + '\t' + uri.encode('utf-8') + '\t' + ' '.join(sentence))
                        i = j-1
                        break
            i += 1

for line in sys.stdin:
    generate_ngrams(line)
