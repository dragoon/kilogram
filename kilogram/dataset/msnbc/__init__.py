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

            # entity is weird
            if values[-1] == '!!!':
                continue

            uri = values[1].replace(' ', '_')
            uri = ner.redirects_file.get(uri, uri)

            truth_data[filename][values[0]] = {'uri': uri, 'exists': True}
            if values[-1] == '---':
                # entity is not in Wikipedia
                truth_data[filename][values[0]]['exists'] = False

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
        visited = set()
        for values in ner_list:
            values['true_uri'] = truth_data[filename].get(values['text'], {'uri': None})
            data[filename].append(values)
            visited.add(values['text'])
        for text, uri in truth_data[filename].iteritems():
            if text not in visited:
                data[filename].append({'text': text, 'context': None, 'type': None, 'true_uri': uri})
    return data
