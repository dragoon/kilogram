# coding=utf-8
from __future__ import division
import pandas as pd
from collections import defaultdict


count_dict = {}
for line in open('predicted_label_counts.txt'):
    uri, label, values = line.split('\t')
    upper_count, lower_count = values.split(',')
    count_dict[(uri, label)] = {'infer_normal': int(upper_count), 'infer_lower': int(lower_count), 'len': len(label.split('_')),
                       'label': label, 'organ_normal': 0, 'organ_lower': 0, 'uri': uri}
for line in open('organic_label_counts.txt'):
    uri, label, values = line.split('\t')
    if (uri, label) in count_dict:
        upper_count, lower_count = values.split(',')
        count_dict[(uri, label)].update({'organ_normal': int(upper_count), 'organ_lower': int(lower_count)})
counts_df = pd.DataFrame(count_dict.values())
del count_dict
print counts_df.head()

"""
We never exclude uppercase labels since we don't match at the beginning of a sentence
"""
includes = open('unambiguous_labels.txt', 'w')
for row in counts_df.iterrows():
    row = row[1]
    exclude = False
    label = row['label']
    uri = row['uri']

    # skip uppercase
    if label.isupper():
        includes.write(label+'\t'+uri+'\n')
        continue
    # if label appears only in lowercase - add to lower includes
    if row['organ_normal'] == 0:  # means label is lowercase
        if row['organ_lower'] > 1:
            includes.write(label+'\t'+uri+'\n')
        continue
    else:
        infer_ratio = row['infer_normal']/(row['infer_lower'] or 1)
        orig_ratio = row['organ_normal']/(row['organ_lower'] or 1)
        if infer_ratio == 0:
            # weird label, p. ex. 中华人民共和国
            continue
        # always write a normal-case label
        includes.write(label+'\t'+uri+'\n')
        if orig_ratio/infer_ratio < 2 and row['infer_lower'] > 0:
            includes.write(label.lower()+'\t'+uri+'\n')
includes.close()
