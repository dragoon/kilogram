"""Retrieves n-gram from HDFS, stores in a format suitable for BerkeleyLM and trains the model."""

import subprocess
import os
import shutil
import argparse

CUR_DIR = os.getcwd()

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--hdfs-dir', dest='hdfs_dir', action='store', required=True,
                    help='path to the HDFS data directory')
parser.add_argument('--src-dir', dest='src_dir', action='store', required=True,
                    help='path to the BerkeleyLM src files (compiled)')
parser.add_argument('-name', dest='model_name', action='store', required=True,
                    help='model file name to store')
parser.add_argument('data_dir', help='directory path to store n-gram data')

args = parser.parse_args()

DATA_DIR = os.path.normpath(args.data_dir)
SRC_DIR = os.path.normpath(args.src_dir)
HDFS_DIR = os.path.normpath(args.hdfs_dir)

if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)
os.makedirs(DATA_DIR)
os.chdir(DATA_DIR)

p = subprocess.Popen('hdfs dfs -cat {0}/*'.format(HDFS_DIR), shell=True, stdout=subprocess.PIPE)

for ngram_type_dir in ('1gms', '2gms', '3gms'):
    if not os.path.exists(ngram_type_dir):
        os.makedirs(ngram_type_dir)

VOCAB_FILE = open('1gms/vocab_cs', 'w')
N_2_FILE = open('2gms/2gm-0001', 'w')
N_3_FILE = open('3gms/3gm-0001', 'w')

print 'Starting HDFS streaming...'
for line in p.stdout:
    line_len = len(line.split('\t')[0].split())
    if line_len == 1:
        VOCAB_FILE.write(line)
    elif line_len == 2:
        N_2_FILE.write(line)
    elif line_len == 3:
        N_3_FILE.write(line)

VOCAB_FILE.close()
N_2_FILE.close()
N_3_FILE.close()

print 'Sorting vocabulary...'
subprocess.call([r"cat 1gms/vocab_cs | sort --parallel=4 -k2,2 -n -r -t $'\t' > vocab_cs1 && mv vocab_cs1 1gms/vocab_cs"], shell=True, executable='/bin/bash')
print 'Removing old vocabulary...'
try:
    os.remove("1gms/vocab_cs.gz")
except OSError:
    pass
print 'Compressing new vocabulary...'
subprocess.call(["gzip 1gms/vocab_cs"], shell=True)

os.chdir(CUR_DIR)
print 'Building the model'
subprocess.call(["java -ea -mx25g -server -cp {0} edu.berkeley.nlp.lm.io.MakeLmBinaryFromGoogle {1} {2}".format([SRC_DIR, DATA_DIR, args.model_name])], shell=True)