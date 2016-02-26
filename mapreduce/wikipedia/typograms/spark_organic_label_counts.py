import sys
from kilogram import ListPacker
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive

ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_2015-04.owl")

# Split each line into words
def unpack_achors(line):
    label, uri_list = line.split('\t')
    # tokenize for commas
    label = ' '.join(tokenize_possessive(default_tokenize_func(label)))
    # should be only one
    uri_counts = ListPacker.unpack(uri_list)
    if len(uri_counts) > 1:
        return
    uri, count = uri_counts[0]
    if uri in ner.dbpedia_types:
        print(label + '\t' + uri + '\t' + count)


for line in sys.stdin:
    unpack_achors(line)
