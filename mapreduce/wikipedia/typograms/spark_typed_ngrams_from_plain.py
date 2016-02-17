"""
hdfs dfs -cat /user/roman/wikipedia_ngrams/* | python spark_typed_ngrams_from_plain.py > typed_ngrams.txt
"""
import sys
from kilogram.dataset.dbpedia import NgramEntityResolver

ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_uri_excludes.txt",
                          "dbpedia_lower_includes.txt", "dbpedia_2015-04.owl")

for line in sys.stdin:
    ngram, count = line.strip().split('\t')
    ngram, count = ' '.join(ner.replace_types(ner.resolve_entities(ngram.split()), order=-1)), int(count)
    if '<dbpedia:' in ngram and len(ngram.split()) <= 3:
        print ngram+'\t'+str(count)
