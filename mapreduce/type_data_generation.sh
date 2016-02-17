#!/usr/bin/env bash

# generate up to 3-grams
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/spark_plain_ngrams.py "/user/roman/dbpedia_data.txt" "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_ngrams" 3