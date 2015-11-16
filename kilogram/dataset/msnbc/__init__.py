from __future__ import division
import os
import re
from kilogram.lang import parse_entities

ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>(\'s)?')


class DataSet(object):
    data_dir = None
    truth_file = None
    ner = None
    truth_data = None
    data = None

    def __init__(self, data_dir, truth_file, ner):
        self.data_dir = data_dir
        self.truth_file = truth_file
        self.ner = ner
        self.truth_data = self._parse_truth_data(ner)
        self.data = self._parse_data()

    def _parse_truth_data(self, ner):
        filename = None
        truth_data = {}
        for line in open(self.truth_file):
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

                if values[-1] == '---':
                    # entity is not in Wikipedia
                    continue
                    truth_data[filename][values[0]]['exists'] = False

                uri = values[1].replace(' ', '_')
                uri = ner.redirects_file.get(uri, uri)

                truth_data[filename][values[0]] = {'uri': uri, 'exists': True}

        return truth_data

    def _parse_data(self):
        data = []
        for filename in os.listdir(self.data_dir):
            text = ' '.join(open(self.data_dir+filename).readlines()).replace('\n', ' ')
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'\'s\b', '', text)
            text = ENTITY_MATCH_RE.sub('\g<2>', text).replace('_', ' ').decode('utf-8')
            datafile = DataFile(filename, text)
            ner_list = parse_entities(text)
            visited = set()
            for values in ner_list:
                values['true_uri'] = self.truth_data[filename].get(values['text'], {'uri': None})
                datafile.add(values)
                visited.add(values['text'])
            for text, uri in self.truth_data[filename].iteritems():
                if text not in visited:
                    datafile.add({'text': text, 'context': None, 'type': None, 'true_uri': uri})
        return data


class DataFile(object):
    filename = None
    _data = None
    text = None

    def __init__(self, filename, text):
        self.filename = filename
        self._data = []
        self.text = text

    def add(self, value):
        self._data.append(value)

    def __iter__(self):
        for elem in self._data:
            yield elem
