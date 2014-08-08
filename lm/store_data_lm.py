"""Retrieves n-gram from HDFS, stores in a format suitable for BerkeleyLM and trains the model."""

import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('data_dir', help='base path to store n-gram data')
parser.add_argument('model_dir', help='base path to store the model')

args = parser.parse_args()

DATA_DIR = args.data_dir
MODEL_DIR = args.model_dir

os.chdir(DATA_DIR)
p = subprocess.Popen('hdfs dfs -cat /user/roman/ngrams_merged/*', shell=True, stdout=subprocess.PIPE)

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
subprocess.call(["cat 1gms/vocab_cs | sort --parallel=4 -k2,2 -n -r -t $'\\t' > vocab_cs1 && mv vocab_cs1 1gms/vocab_cs"], shell=True)
print 'Removing old vocabulary...'
try:
    os.remove("1gms/vocab_cs.gz")
except OSError:
    pass
print 'Compressing new vocabulary...'
subprocess.call(["gzip 1gms/vocab_cs"], shell=True)

os.chdir(MODEL_DIR)
print 'Building the model'
subprocess.call(["java -ea -mx25g -server -cp ./src edu.berkeley.nlp.lm.io.MakeLmBinaryFromGoogle {0} google_model_types.bin".format(DATA_DIR)], shell=True)