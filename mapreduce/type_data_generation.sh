#!/usr/bin/env bash

# generate up to 3-grams
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/typograms/spark_plain_ngrams.py "/user/roman/dbpedia_data.txt" "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_ngrams" 3

# compute organic link counts
spark-submit --master yarn-client --num-executors 10 --executor-memory 3g ./wikipedia/typograms/spark_organic_ngram_counts.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/organic_ngram_counts"