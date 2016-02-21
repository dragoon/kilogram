import sys
from pyspark import SparkContext
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.ngram_service import SUBSTITUTION_TOKEN, ListPacker

sc = SparkContext(appName="SparkGenerateTypedNgrams")

lines = sc.textFile(sys.argv[1])

ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_2015-04.owl")
# free some space
ner.redirects_file = None

def generate_typed_ngram(line):
    result = []
    ngram, count = line.split('\t')
    type_count = ngram.count('<dbpedia:')
    ngram = ngram.split()
    ngram = [ner.get_type(w, -1) if w.startswith('<dbpedia:') else w for w in ngram]
    if type_count > 0:
        type_indexes = [i for i, x in enumerate(ngram) if x.startswith('<dbpedia:')]
        for type_index in type_indexes:
            subst_ngram = ngram[:]
            entity_type = ngram[type_index]
            subst_ngram[type_index] = SUBSTITUTION_TOKEN
            new_ngram = " ".join(subst_ngram)
            result.append((new_ngram, {entity_type: int(count)}))

def collect_counts(value1, value2):
    for label, count in value1.items():
        if label in value2:
            value2[label] += count
        else:
            value2[label] = count
    return value2


typed_ngrams = lines.map(generate_typed_ngram).filter(lambda x: x[1] >= 10).reduceByKey(collect_counts)


def printer(value):
    return value[0] + '\t' + ListPacker.pack(value[1].items())

typed_ngrams.map(printer).saveAsTextFile(sys.argv[2])
