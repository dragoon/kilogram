#!/usr/bin/env python
"""
Runs hadoop job to process Google Books N-grams
"""

import argparse
import os.path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-cmd', '--extra-command', nargs='*', dest='extra_commands', default=[],
                    action='store', help='extra commands to execute before starting the job')
parser.add_argument('-file', '--extra-file', nargs='*', dest='extra_files', default=[],
                    action='store', help='extra file to copy with the job')
parser.add_argument('--filter-file', dest='filter_file', required=False,
                    action='store', help='filter file to copy with the job')
parser.add_argument('-r', '--reducers', dest='reducers_num', action='store', required=False,
                    default=20, type=int, help='number of reducers')
parser.add_argument('-m', '--mapper', dest='mapper', action='store', default='mapper_generic.py',
                    help='path to the mapper script')
parser.add_argument('-n', dest='n', action='store', type=int, required=False, default=0,
                    help='size of n-gram to extract (when doing filter)')
parser.add_argument('input', help='input path on HDFS')
parser.add_argument('output', help='output path on HDFS, removed before starting the job')

args = parser.parse_args()
#print args

MAPPER_PATH = args.mapper
MAPPER = os.path.basename(MAPPER_PATH)

# Remove output if exists
subprocess.call(["hadoop fs -rm -r {0}".format(args.output)], shell=True)

for command in args.extra_commands:
    subprocess.call([command], shell=True)

extra_files = ''
if args.extra_files:
    extra_files = ',' + ','.join(args.extra_files)
if args.filter_file:
    extra_files += ',' + args.filter_file
    filter_file = os.path.basename(args.filter_file)
else:
    filter_file = ''

hadoop_cmd = """hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.job.contract=false \
  -Dmapreduce.job.reduces={reducers} \
  -files {mapper_path},reducer_generic.py{extra_files} \
  -cmdenv NGRAM={n} \
  -cmdenv FILTER_FILE={filter_file} \
  -mapper {mapper} \
  -reducer reducer_generic.py \
  -input {0} -output {1}""".format(args.input, args.output, mapper_path=MAPPER_PATH, mapper=MAPPER,
                                   reducers=args.reducers_num, extra_files=extra_files, n=args.n,
                                   filter_file=filter_file)

subprocess.call(hadoop_cmd, shell=True)