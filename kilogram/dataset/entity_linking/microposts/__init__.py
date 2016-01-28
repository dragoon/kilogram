from __future__ import division
from entity_linking import CandidateEntity
from kilogram.lang import parse_entities


class DataSet(object):
    dataset_file = None
    ner = None
    data = None

    def __init__(self, dataset_file, ner):
        self.ner = ner
        self.dataset_file = dataset_file
        self.data = self._parse_data()

    def _parse_data(self):
        data = []
        for line in open(self.dataset_file):
            line = line.strip().split('\t')
            try:
                datafile = DataFile(line[0], line[1])
            except IndexError:
                continue
            truth_data = dict(zip(line[2::2], [x.replace('http://dbpedia.org/resource/', '') for x in line[3::2]]))
            ner_list = parse_entities(line[1].decode('utf-8'))
            visited = set()
            for values in ner_list:
                candidate = CandidateEntity(0, 0, values['text'], e_type=values['type'],
                                            context=values['context'], ner=self.ner)
                candidate.truth_data = {'uri': truth_data.get(values['text']), 'exists': True}
                datafile.candidates.append(candidate)
                visited.add(values['text'])
            for text, uri in truth_data.iteritems():
                if text not in visited:
                    candidate = CandidateEntity(0, 0, text, ner=self.ner)
                    candidate.truth_data = {'uri': uri, 'exists': True}
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
