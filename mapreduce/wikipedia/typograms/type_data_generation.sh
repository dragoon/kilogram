#!/usr/bin/env bash


### Compute organic link counts
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/spark_organic_label_counts.py > organic_label_counts.txt
hdfs dfs -rm -r /user/roman/predicted_label_counts
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
hdfs dfs -cat /user/roman/predicted_label_counts/* > predicted_label_counts.txt

### Generate unambiguous_labels.txt (only for typed entities)
python ./wikipedia/typograms/generate_unambiguous_labels.py > unambiguous_labels.txt


### Generate up to 3-grams
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files unambiguous_labels.txt ./wikipedia/typograms/spark_generate_linked_ngrams.py "/data/wikipedia_plaintext" "/user/roman/wikipedia_linked_ngrams" 3
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files dbpedia_data.txt,dbpedia_2015-04.owl ./wikipedia/typograms/spark_generate_typograms.py "/user/roman/wikipedia_linked_ngrams" "/user/roman/wikipedia_typed_ngrams"


### Put into Hbase:
pig -p table=typogram -p path=/user/roman/hbase_wikipedia_typed_ngrams ../extra/hbase_upload_array.pig


# EVALUATION
bzcat ../../datasets/wikipedia/enwiki-20150602-pages-articles.xml.bz2 | python WikiExtractor.py > out.txt &
# randomly select 20 articles from Wikipedia, output to EVAL
python ../wikipedia/typograms/evaluation/select_random_articles.py -n 20 --output_dir EVAL
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.99
python -m wikipedia.typograms.evaluation.evaluate  --gold-file gold_typogram.tsv --eval-dir EVAL