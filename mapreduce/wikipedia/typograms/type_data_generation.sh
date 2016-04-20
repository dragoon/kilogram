#!/usr/bin/env bash


### Compute organic link counts
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_organic_label_counts.py > organic_label_counts.txt
hdfs dfs -rm -r /user/roman/predicted_label_counts
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
hdfs dfs -cat /user/roman/predicted_label_counts/* > predicted_label_counts.txt

### Generate unambiguous_labels.txt (only for typed entities)
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 5 > unambiguous_labels5.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 8 > unambiguous_labels8.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 10 > unambiguous_labels10.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 20 > unambiguous_labels20.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 30 > unambiguous_labels30.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 40 > unambiguous_labels40.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 50 > unambiguous_labels50.txt


# EVALUATION
bzcat ../../datasets/wikipedia/enwiki-20150602-pages-articles.xml.bz2 | python WikiExtractor.py > out.txt &
# randomly select 30 articles from Wikipedia, output to EVAL
python ../wikipedia/typograms/evaluation/select_random_articles.py -n 30 --output_dir EVAL
# evaluation
python -m wikipedia.typograms.evaluation.evaluate  --gold-file gold_typogram.tsv --eval-dir EVAL
## END: EVALUATION


mkdir unambig_percentile
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.99 --min-count 2 > unambig_percentile/unambiguous_percentile_labels99_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.95 --min-count 2 > unambig_percentile/unambiguous_percentile_labels95_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.90 --min-count 2 > unambig_percentile/unambiguous_percentile_labels90_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.85 --min-count 2 > unambig_percentile/unambiguous_percentile_labels85_2.tsv
cd unambig_percentile


hdfs dfs -rm -r /user/roman/predicted_label_counts_percentile
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ../wikipedia/typograms/spark_predicted_label_counts.py "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts_percentile"
hdfs dfs -cat /user/roman/predicted_label_counts_percentile/* > predicted_label_counts.txt

python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels99_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 20 > unambiguous_percentile_labels99_2_ratio20.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels99_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 10 > unambiguous_percentile_labels99_2_ratio10.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels99_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 5 > unambiguous_percentile_labels99_2_ratio5.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels95_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 5 > unambiguous_percentile_labels95_2_ratio5.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels99_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 50 > unambiguous_percentile_labels99_2_ratio50.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels95_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 10 > unambiguous_percentile_labels95_2_ratio10.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels95_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 20 > unambiguous_percentile_labels95_2_ratio20.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels95_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 50 > unambiguous_percentile_labels95_2_ratio50.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels90_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 50 > unambiguous_percentile_labels90_2_ratio50.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels90_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 20 > unambiguous_percentile_labels90_2_ratio20.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels90_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 10 > unambiguous_percentile_labels90_2_ratio10.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels90_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 5 > unambiguous_percentile_labels90_2_ratio5.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels85_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 5 > unambiguous_percentile_labels85_2_ratio5.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels85_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 10 > unambiguous_percentile_labels85_2_ratio10.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels85_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 20 > unambiguous_percentile_labels85_2_ratio20.tsv
python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels85_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit 50 > unambiguous_percentile_labels85_2_ratio50.tsv



## Different PROJECT
### Generate up to 3-grams
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files unambiguous_labels.txt ./wikipedia/typograms/spark_generate_linked_ngrams.py "/data/wikipedia_plaintext" "/user/roman/wikipedia_linked_ngrams" 3
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files dbpedia_data.txt,dbpedia_2015-04.owl ./wikipedia/typograms/spark_generate_typograms.py "/user/roman/wikipedia_linked_ngrams" "/user/roman/wikipedia_typed_ngrams"


### Put into Hbase:
pig -p table=typogram -p path=/user/roman/hbase_wikipedia_typed_ngrams ../extra/hbase_upload_array.pig