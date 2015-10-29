from __future__ import division
import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import parse_data
from entity_linking.priorprob import get_max_uri, get_max_typed_uri
from kilogram import NgramService
import kilogram


class TestEntityLinking(unittest.TestCase):
    ner = None
    msnbc_data = None

    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', '9090'))
        kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
        self.ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_types.txt",
                                  "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                                  "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                                  "/Users/dragoon/Downloads/dbpedia/dbpedia_redirects.txt",
                                  "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
        self.msnbc_data = parse_data('../extra/data/msnbc/', self.ner)

    def test_prior_prob_d2kb(self):
        print 'Prior prob'
        tp = fp = fn = tn = 0
        for filename, values in self.msnbc_data.iteritems():
            for index_tuple, line_dict in values.iteritems():
                true_uri = line_dict['uri']
                text = line_dict['text']
                uri = get_max_uri(text)

                if uri == true_uri:
                    tp += 1
                elif uri is None and true_uri != 'NONE':
                    fn += 1
                else:
                    fp += 1
        precision = tp / (tp+fp)
        recall = tp / (tp+fn)
        print 'P =', precision, 'R =', recall, 'F1 =', 2*precision*recall/(precision+recall)
        print tp, fp, fn, tn

    def test_prior_prob_d2kb_typed(self):
        print 'Prior prob + type improvements'
        tp = fp = fn = tn = 0
        for filename, values in self.msnbc_data.iteritems():
            for index_tuple, line_dict in values.iteritems():
                true_uri = line_dict['uri']
                text = line_dict['text']

                e_type = line_dict['ner']
                if e_type == '<LOCATION>':
                    e_type = '<dbpedia:Place>'
                elif e_type == '<PERSON>':
                    e_type = '<dbpedia:Person>'
                elif e_type in ('<ORGANISATION>', '<ORGANIZATION>'):
                    e_type = '<dbpedia:Organisation>'
                if e_type is not None:
                    uri = get_max_typed_uri(text, e_type, self.ner)
                else:
                    uri = get_max_uri(text)

                if uri == true_uri:
                    tp += 1
                elif uri is None and true_uri != 'NONE':
                    fn += 1
                else:
                    fp += 1
        precision = tp / (tp+fp)
        recall = tp / (tp+fn)
        print 'P =', precision, 'R =', recall, 'F1 =', 2*precision*recall/(precision+recall)
        print tp, fp, fn, tn


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
