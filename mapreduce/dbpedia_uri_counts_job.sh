#!/bin/bash
REDUCERS=${1:-20}
hadoop fs -rm -r /user/roman/ngrams_dbpedia_counts
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files mapper_dbpedia_uri_counts.py,reducer_generic.py,dbpedia.dbm \
  -mapper mapper_dbpedia_uri_counts.py \
  -reducer reducer_generic.py \
  -input /user/roman/ngrams_dbpedia/ -output /user/roman/ngrams_dbpedia_counts
