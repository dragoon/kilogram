#!/bin/bash
REDUCERS=${1:-20}
hadoop fs -rm -r /user/roman/ngrams_filtered
cp ../extra/cities15000.zip .
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files mapper_generic.py,reducer_generic.py,cities15000.zip \
  -mapper mapper_generic.py \
  -reducer reducer_generic.py \
  -input /data/ngrams -output /user/roman/ngrams_filtered
rm cities15000.zip