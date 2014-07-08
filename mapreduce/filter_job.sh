#!/bin/bash

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our own variables:
N=0
REDUCERS=20
FILTER_FILE=""

while getopts "r:n:f:" opt; do
    case "$opt" in
    n)  N=$OPTARG
        ;;
    r)  REDUCERS=$OPTARG
        ;;
    f) FILTER_FILE=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

hadoop fs -rm -r $2
if [[ $FILTER_FILE ]]; then
    cp $FILTER_FILE filter_file.txt
else
    touch filter_file.txt
if

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces=$REDUCERS \
  -files ./filter/mapper_filter.py,reducer_generic.py,filter_file.txt \
  -cmdenv NGRAM=$N \
  -cmdenv FILTER_FILE=filter_file.txt \
  -mapper mapper_filter.py \
  -reducer reducer_generic.py \
  -input $1 -output $2

rm filter_file.txt
