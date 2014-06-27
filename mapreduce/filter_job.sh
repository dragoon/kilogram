#!/bin/bash
REDUCERS=${1:-20}
hadoop fs -rm -r /user/roman/ngrams_filtered
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files ./mapper_generic.py,./reducer_generic.py \
  -mapper mapper_generic.py \
  -reducer reducer_generic.py \
  -input /data/ngrams -output /user/roman/ngrams_filtered