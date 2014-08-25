import sys
import shelve
import anydbm

from kilogram.dataset.wikipedia.entities import parse_types_text


dbpedia_redirects = anydbm.open('dbpedia_redirects.dbm', 'r')
dbpedia_types = shelve.open('dbpedia_types.dbm', flag='r')

for line in sys.stdin:
    if not line:
        continue
    line = parse_types_text(line, dbpedia_redirects, dbpedia_types, numeric=False)
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


dbpedia_redirects.close()
dbpedia_types.close()