import sys
from kilogram.dataset.wikipedia.entities import parse_types_text

dbpedia_types = {}
for line in open('dbpedia_types.tsv'):
    label, uri, types = line.strip().split('\t')
    dbpedia_types[label] = types.split(';')

for line in sys.stdin:
    if not line:
        continue
    line = parse_types_text(line, dbpedia_types, numeric=False)
    sentences = line.split(' . ')
    last = len(sentences) - 1
    for i, sentence in enumerate(sentences):
        try:
            unicode(sentence)
        except UnicodeDecodeError:
            continue
        if i == last and not sentence.endswith('.'):
            continue
        if not sentence.endswith('.'):
            print sentence+' .'
        else:
            print sentence
