#!/bin/bash
REDUCERS=${3:-20}
hadoop fs -rm -r $2
cp ../extra/cities15000.zip .
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files mapper_generic.py,reducer_generic.py,cities15000.zip \
  -mapper mapper_generic.py \
  -reducer reducer_generic.py \
  -input $1 -output $2
rm cities15000.zip