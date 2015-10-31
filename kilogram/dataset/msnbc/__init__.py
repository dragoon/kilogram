from __future__ import division
import os
import re
from kilogram.lang import parse_entities

ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>(\'s)?')


def _parse_truth_data(truth_file, ner):
    filename = None
    truth_data = {}
    for line in open(truth_file):
        if line.startswith("~~~ "):
            filename = line.strip().split()[-1]
            truth_data[filename] = {}
        elif line.startswith("### "):
            pass
        else:
            values = line.strip().split('\t')
            # entity is not in Wikipedia
            if values[-1] == '---':
                uri = ner.redirects_file.get(values[1], values[1])
                truth_data[filename][values[0]] = {'uri': uri, 'exists': False}
            # entity is weird
            elif values[-1] == '!!!':
                continue
            else:
                uri = ner.redirects_file.get(values[1], values[1])
                truth_data[filename][values[0]] = {'uri': uri, 'exists': True}
    return truth_data


def parse_data(data_dir, truth_file, ner):
    truth_data = _parse_truth_data(truth_file, ner)
    data = {}
    for filename in os.listdir(data_dir):
        data[filename] = []
        text = ' '.join(open(data_dir+filename).readlines()).replace('\n', ' ')
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\'s\b', '', text)
        ner_list = parse_entities(ENTITY_MATCH_RE.sub('\g<2>', text).replace('_', ' ').decode('utf-8'))
        for values in ner_list:
            values['uri'] = truth_data[filename].get(values['text'])
            data[filename].append(values)
    return data
