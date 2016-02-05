from __future__ import division
import codecs
from entity_linking import CandidateEntity, Entity


class DataSet(object):
    ner = None
    data = None
    handles_dict = None
    truth_data = None

    def __init__(self, dataset_file, ner, handles_file=None, truth_file=None):
        self.ner = ner
        if handles_file:
            self.handles_dict = dict([x.split('\t') for x in codecs.open(handles_file, 'r', 'utf-8').read().splitlines()])
        else:
            self.handles_dict = {}
        self.truth_data = self._parse_truth_data(truth_file)
        self.data = self._parse_data(dataset_file)

    def _astrology_uri(self, cand_string):
        signs = {'Leo', 'Capricorn', 'Pisces', 'Libra', 'Virgo', 'Cancer', 'Aquarius',
                 'Gemini', 'Taurus', 'Aries', 'Sagittarius', 'Scorpio'}
        if cand_string.title() in signs:
            return cand_string.title()+'_(astrology)'
        if cand_string[:-1].title() in signs:
            return cand_string[:-1].title()+'_(astrology)'
        if cand_string == 'Aquarians':
            return 'Aquarius_(astrology)'
        return None

    def _parse_truth_data(self, truth_file):
        data = {}
        for line in codecs.open(truth_file, 'r', 'utf-8'):
            line = line.strip().split('\t')
            try:
                data[line[0]] = {}
            except IndexError:
                continue
            truth_data = dict(zip(line[2::2], [x.encode('utf-8').replace('http://dbpedia.org/resource/', '') for x in line[3::2]]))
            for text, uri in truth_data.iteritems():
                data[line[0]][text] = self.ner.redirects_file.get(uri, uri)
        return data

    def _parse_data(self, dataset_file):
        data = {}
        for line in codecs.open(dataset_file, 'r', 'utf-8'):
            line = line.strip().split('\t')
            try:
                if line[0] not in data:
                    data[line[0]] = datafile = DataFile(line[0], None)
                else:
                    datafile = data[line[0]]
            except IndexError:
                continue
            truth_data = self.truth_data[datafile.filename]
            start_i, end_i, mention = line[1:4]
            try:
                candidates = [x.rsplit(',', 1) for x in line[4].split()]
            except:
                continue
            candidates = [(uri, float(count)) for uri, count in candidates]

            cand_string = self.handles_dict.get(mention, mention)
            candidate = CandidateEntity(int(start_i), int(end_i), cand_string,
                                        ner=self.ner, context='TWITTER', candidates=candidates)
            astr_uri = self._astrology_uri(candidate.cand_string)
            if astr_uri:
                candidate.entities = [Entity(astr_uri, 1, self.ner)]
            candidate.truth_data = {'uri': truth_data.get(mention), 'exists': True}
            datafile.candidates.append(candidate)
            if mention in truth_data:
                del truth_data[mention]

        for filename, truth_data in self.truth_data.iteritems():
            for text, uri in truth_data.items():
                candidate = CandidateEntity(0, 0, text, ner=self.ner)
                astr_uri = self._astrology_uri(candidate.cand_string)
                if astr_uri:
                    candidate.entities = [Entity(astr_uri, 1, self.ner)]
                candidate.truth_data = {'uri': uri, 'exists': True}
                if filename not in data:
                    data[filename] = DataFile(filename, None)
                data[filename].candidates.append(candidate)
        return data.values()


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
