#!/bin/bash
REDUCERS=${1:-20}
hadoop fs -rm -r /home/roman/ngrams_filtered
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files /home/roman/mapper.py,/home/roman/reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input /data/ngrams -output /user/roman/ngrams_filtered