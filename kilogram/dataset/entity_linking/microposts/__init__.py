from __future__ import division
import codecs
from entity_linking import CandidateEntity, Entity
from kilogram.lang import parse_entities, parse_tweet_entities, strip_tweet_entities


class DataSet(object):
    dataset_file = None
    ner = None
    data = None
    handles_dict = None

    def __init__(self, dataset_file, ner, handles_file=None):
        self.ner = ner
        self.dataset_file = dataset_file
        if handles_file:
            self.handles_dict = dict([x.split('\t') for x in codecs.open(handles_file, 'r', 'utf-8').read().splitlines()])
        else:
            self.handles_dict = {}
        self.data = self._parse_data()

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

    def _parse_data(self):
        data = []
        for line in codecs.open(self.dataset_file, 'r', 'utf-8'):
            line = line.strip().split('\t')
            try:
                datafile = DataFile(line[0], line[1])
            except IndexError:
                continue
            truth_data = dict(zip(line[2::2], [x.encode('utf-8').replace('http://dbpedia.org/resource/', '') for x in line[3::2]]))
            tweet_ne_list = parse_tweet_entities(datafile.text)
            tweet_ne_names = set([x['text'] for x in tweet_ne_list])
            ner_list = parse_entities(strip_tweet_entities(datafile.text))
            ner_list = [x for x in ner_list if x['text'] not in tweet_ne_names] + tweet_ne_list

            visited = set()
            for values in ner_list:
                cand_string = self.handles_dict.get(values['text'], values['text'].decode('utf-8'))
                candidate = CandidateEntity(0, 0, cand_string, e_type=values['type'],
                                            context=values['context'], ner=self.ner)
                astr_uri = self._astrology_uri(candidate.cand_string)
                if astr_uri:
                    candidate.entities = [Entity(astr_uri, 1, self.ner)]
                candidate.truth_data = {'uri': truth_data.get(values['text']), 'exists': True}
                datafile.candidates.append(candidate)
                visited.add(values['text'])
            for text, uri in truth_data.iteritems():
                if text not in visited:
                    candidate = CandidateEntity(0, 0, text, ner=self.ner)
                    astr_uri = self._astrology_uri(candidate.cand_string)
                    if astr_uri:
                        candidate.entities = [Entity(astr_uri, 1, self.ner)]
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
