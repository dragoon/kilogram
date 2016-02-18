#!/usr/bin/env bash


# compute organic link counts
spark-submit --master yarn-client --num-executors 10 --executor-memory 3g ./wikipedia/typograms/spark_organic_label_counts.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/organic_label_counts"
hdfs dfs -cat /user/roman/organic_label_counts/* > organic_label_counts.txt
spark-submit --executor-memory 3g --num-executors 10 --master yarn-client ./wikipedia/typograms/spark_predicted_label_counts.py "/user/roman/organic_label_counts" "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"


# generate up to 3-grams
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/typograms/spark_plain_ngrams.py "/user/roman/dbpedia_data.txt" "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_ngrams" 3