from __future__ import division
import os
import re
from ....entity_linking import CandidateEntity
from ....lang import parse_entities


class DataSet(object):
    data_dir = None
    ner = None
    truth_data = None
    data = None

    def __init__(self, data_dir, truth_file, ner):
        self.data_dir = data_dir
        self.ner = ner
        self.truth_data = self._parse_truth_data(truth_file, ner)
        self.data = self._parse_data()

    def _parse_truth_data(self, truth_file, ner):
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
            if filename not in self.truth_data:
                continue
            text = ' '.join(open(self.data_dir+filename).readlines()).replace('\n', ' ')
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'\'s\b', '', text).decode('utf-8')
            datafile = DataFile(filename, text)
            ner_list = parse_entities(text)
            visited = set()
            for values in ner_list:
                candidate = CandidateEntity(0, 0, values['text'], e_type=values['type'],
                                            context=values['context'], ner=self.ner)
                candidate.truth_data = self.truth_data[filename].get(values['text'], {'uri': None})
                datafile.candidates.append(candidate)
                visited.add(values['text'])
            for text, uri in self.truth_data[filename].iteritems():
                if text not in visited:
                    candidate = CandidateEntity(0, 0, text, ner=self.ner)
                    candidate.truth_data = uri
                    datafile.candidates.append(candidate)
            data.append(datafile)
        return data


class DataFile(object):
    filename = None
    candidates = None
    text = None

    def __init__(self, filename, text):
        self.filename = filename
        self.candidates = []
        self.text = text

    def __iter__(self):
        for elem in self.candidates:
            yield elem
