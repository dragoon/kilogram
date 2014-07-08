#!/bin/bash

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our own variables:
REDUCERS=20

while getopts "r:" opt; do
    case "$opt" in
    r)  REDUCERS=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

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