cd /tmp
git clone https://github.com/dragoon/kilogram.git
cd kilogram/mapreduce

# dbpedia_data generation
python dbpedia_dbp.py


# wiki_urls and wiki_achors generation
hdfs dfs -rm -r /user/roman/wiki_anchors /user/roman/wiki_urls
spark-submit --executor-memory 5g --num-executors 20 --master yarn-client ./wikipedia/spark_anchors.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wiki_anchors" "/user/roman/wiki_urls"
echo "disable 'wiki_anchors'" | hbase shell -n
echo "drop 'wiki_anchors'" | hbase shell -n
echo "create 'wiki_anchors', 'ngram'" | hbase shell -n
pig -p table=wiki_anchors -p path=/user/roman/wiki_anchors ../extra/hbase_upload_array.pig
echo "disable 'wiki_urls'" | hbase shell -n
echo "drop 'wiki_urls'" | hbase shell -n
echo "create 'wiki_urls', 'ngram'" | hbase shell -n
pig -p table=wiki_urls -p path=/user/roman/wiki_urls ../extra/hbase_upload_array.pig

# candidate_ngrams generation - depends on prev task
spark-submit --master yarn-client --num-executors 10 --executor-memory 5g ./entity_linking/spark_candidate_ngrams.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/SOTA_EL/candidate_ngram_links"
echo "disable 'wiki_anchor_ngrams'" | hbase shell -n
echo "drop 'wiki_anchor_ngrams'" | hbase shell -n
echo "create 'wiki_anchor_ngrams', 'ngram'" | hbase shell -n
pig -p table=wiki_anchor_ngrams -p path=/user/roman/SOTA_EL/candidate_ngram_links ../extra/hbase_upload_array.pig


# wiki direct links table
spark-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks.py "/user/michael/plain_wikipedia_pagelinks" "/user/roman/SOTA_EL/wikipedia_pagelinks"
echo "disable 'wiki_direct_links'" | hbase shell -n
echo "drop 'wiki_direct_links'" | hbase shell -n
echo "create 'wiki_direct_links', 'ngram'" | hbase shell -n
pig -p table=wiki_direct_links -p path=/user/roman/SOTA_EL/wikipedia_pagelinks ../extra/hbase_upload_array.pig


park-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks_mentions.py "/user/ded/wikilinks_mentions" "/user/roman/SOTA_EL/wikipedia_pagelinks_mentions"
echo "disable 'wiki_direct_links_mentions'" | hbase shell -n
echo "drop 'wiki_direct_links_mentions'" | hbase shell -n
echo "create 'wiki_direct_links_mentions', 'ngram'" | hbase shell -n
pig -p table=wiki_direct_links_mentions -p path=/user/roman/SOTA_EL/wikipedia_pagelinks_mentions ../extra/hbase_upload_array.pig


rm -rf kilogram
