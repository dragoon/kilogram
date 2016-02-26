# coding=utf-8
from __future__ import division
import argparse
import re
import pandas as pd

NON_CHAR_RE = re.compile(r'[\W\d]+$')

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--ratio-limit', default=20, type=int,
                    help='ratio limit for labels')

args = parser.parse_args()


count_dict = {}
for line in open('predicted_label_counts.txt'):
    label, uri, count = line.split('\t')
    count_dict[(uri, label)] = {'infer_count': int(count), 'len': len(label.split('_')),
                       'label': label, 'organ_count': 0, 'uri': uri}
for line in open('organic_label_counts.txt'):
    label, uri, count = line.split('\t')
    if (uri, label) in count_dict:
        count_dict[(uri, label)].update({'organ_count': int(count)})
counts_df = pd.DataFrame(count_dict.values())
del count_dict


for row in counts_df.iterrows():
    row = row[1]
    exclude = False
    label = row['label']
    uri = row['uri']

    if NON_CHAR_RE.match(label):
        continue

    # skip uppercase
    if label.isupper():
        print(label+'\t'+uri)
        continue
    # write a normal-case label if ratio is less than 20 (means we do not link something that suddenly become super popular - probably an error)
    if row['organ_count'] > 1 and row['infer_count']/(row['organ_count']) < args.ratio_limit:
        print(label+'\t'+uri)
