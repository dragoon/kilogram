#!/usr/bin/env bash


### Compute organic link counts
spark-submit --master yarn-client --num-executors 10 --executor-memory 3g ./wikipedia/typograms/spark_organic_label_counts.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/organic_label_counts"
hdfs dfs -cat /user/roman/organic_label_counts/* > organic_label_counts.txt
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
hdfs dfs -cat /user/roman/predicted_label_counts/* > predicted_label_counts.txt

### Generate unambiguous_labels.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py


### Generate up to 3-grams
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./wikipedia/typograms/spark_generate_typograms.py "/user/roman/dbpedia_data.txt" "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_typed_ngrams" 3


../../run_job.py -m ./type_prediction/mapper.py -r ./type_prediction/reducer.py "/user/roman/wikipedia_typed_ngrams" "/user/roman/hbase_wikipedia_typed_ngrams"

### Put into Hbase:
pig -p table=typogram -p path=/user/roman/hbase_wikipedia_typed_ngrams ../extra/hbase_upload_array.pig