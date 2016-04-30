#!/usr/bin/env bash

### Generate all counts of labels, including ambiguous (to check most popular labels)
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_organic_label_counts_all.py > organic_label_counts_all.txt
hdfs dfs -rm -r /user/roman/predicted_label_counts_all
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts_all.txt ./wikipedia/typograms/spark_predicted_label_counts.py "organic_label_counts_all.txt" "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts_all"
hdfs dfs -cat /user/roman/predicted_label_counts_all/* > predicted_label_counts_all.txt


### Compute organic link counts
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_organic_label_counts.py > organic_label_counts.txt
hdfs dfs -rm -r /user/roman/predicted_label_counts
spark-submit --executor-memory 5g --num-executors 10 --master yarn-client --files organic_label_counts.txt ./wikipedia/typograms/spark_predicted_label_counts.py "organic_label_counts.txt" "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts"
hdfs dfs -cat /user/roman/predicted_label_counts/* > predicted_label_counts.txt

### Generate unambiguous_labels.txt (only for typed entities)
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 1 > unambiguous_labels1.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 2 > unambiguous_labels2.txt
python ./wikipedia/typograms/generate_unambiguous_labels.py --ratio-limit 3 > unambiguous_labels3.txt
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
python wikipedia/typograms/evaluation/evaluate.py  --gold-file gold_typogram.tsv --eval-dir EVAL
## END: EVALUATION


mkdir unambig_percentile
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.99 --min-count 2 > unambig_percentile/unambiguous_percentile_labels99_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.999 --min-count 2 > unambig_percentile/unambiguous_percentile_labels999_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.975 --min-count 2 > unambig_percentile/unambiguous_percentile_labels975_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.95 --min-count 2 > unambig_percentile/unambiguous_percentile_labels95_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.90 --min-count 2 > unambig_percentile/unambiguous_percentile_labels90_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.85 --min-count 2 > unambig_percentile/unambiguous_percentile_labels85_2.tsv
hdfs dfs -cat /user/roman/wiki_anchors/* | python wikipedia/typograms/generate_unambiguous_percentile_labels.py --percentile 0.80 --min-count 2 > unambig_percentile/unambiguous_percentile_labels80_2.tsv
cd unambig_percentile


hdfs dfs -rm -r /user/roman/predicted_label_counts_percentile
spark-submit --executor-memory 5g --num-executors 12 --master yarn-client --files unambig_percentile/unambiguous_percentile_labels80_2.tsv ../wikipedia/typograms/spark_predicted_label_counts.py "unambiguous_percentile_labels80_2.tsv" "/data/wikipedia_plaintext" "/user/roman/predicted_label_counts_percentile"
hdfs dfs -cat /user/roman/predicted_label_counts_percentile/* > predicted_label_counts.txt

for percentile in 80 85 90 95 975 99 999; do
    for ratio in 1 3 5 10 20 50; do
        python ../wikipedia/typograms/generate_unambiguous_labels.py --organic-file unambiguous_percentile_labels${percentile}_2.tsv --predicted-file predicted_label_counts.txt --ratio-limit ${ratio} > unambiguous_percentile_labels${percentile}_2_ratio${ratio}.tsv
    done
done




## Different PROJECT
### Generate up to 3-grams
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files unambiguous_labels.txt ./wikipedia/typograms/spark_generate_linked_ngrams.py "/data/wikipedia_plaintext" "/user/roman/wikipedia_linked_ngrams" 3
spark-submit --num-executors 10 --executor-memory 5g --master yarn-client --files dbpedia_data.txt,dbpedia_2015-04.owl ./wikipedia/typograms/spark_generate_typograms.py "/user/roman/wikipedia_linked_ngrams" "/user/roman/wikipedia_typed_ngrams"


### Put into Hbase:
pig -p table=typogram -p path=/user/roman/hbase_wikipedia_typed_ngrams ../extra/hbase_upload_array.pig