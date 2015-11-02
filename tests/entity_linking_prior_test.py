from __future__ import division
import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import parse_data
from entity_linking.priorprob import get_max_uri, get_max_typed_uri
from kilogram import NgramService
import kilogram

NgramService.configure(hbase_host=('diufpc304', '9090'))
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_types.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_redirects.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
msnbc_data = parse_data('../extra/data/msnbc/texts/',
                        '../extra/data/msnbc/msnbc_truth.txt', ner)


class Metrics(object):
    fp = None
    tp = None
    fn = None
    tn = None

    def __init__(self):
        self.tp = 0
        self.fp = 0
        self.fn = 0
        self.tn = 0

    def evaluate(self, true_uri, uri):
        if uri == true_uri['uri']:
            self.tp += 1
        elif uri is None and true_uri['uri'] is not None:
            if true_uri['exists']:
                self.fn += 1
        else:
            self.fp += 1

    def print_metrics(self):
        precision = self.tp / (self.tp+self.fp)
        recall = self.tp / (self.tp+self.fn)
        print 'P =', precision, 'R =', recall, 'F1 =', 2*precision*recall/(precision+recall)
        print self.tp, self.fp, self.fn, self.tn


class TestEntityLinking(unittest.TestCase):

    def test_prior_prob_d2kb(self):
        print 'Prior prob, D2KB'
        metric = Metrics()
        for filename, values in msnbc_data.iteritems():
            for line_dict in values:
                # D2KB condition
                if line_dict['true_uri']['uri'] is None:
                    continue
                true_uri = line_dict['true_uri']
                text = line_dict['text']
                uri = get_max_uri(text)

                metric.evaluate(true_uri, uri)
        metric.print_metrics()

    def test_prior_prob_a2kb(self):
        print 'Prior prob, A2KB'
        metric = Metrics()
        for filename, values in msnbc_data.iteritems():
            for line_dict in values:
                true_uri = line_dict['true_uri']
                text = line_dict['text']
                uri = get_max_uri(text)

                metric.evaluate(true_uri, uri)
        metric.print_metrics()

    def test_prior_prob_d2kb_typed(self):
        print 'Prior prob + type improvements, D2KB'
        metric = Metrics()
        for filename, values in msnbc_data.iteritems():
            for line_dict in values:
                # D2KB
                if line_dict['true_uri']['uri'] is None:
                    continue
                true_uri = line_dict['true_uri']
                text = line_dict['text']

                e_type = line_dict['type']
                if e_type is not None:
                    uri = get_max_typed_uri(text, e_type, ner)
                else:
                    uri = get_max_uri(text)

                metric.evaluate(true_uri, uri)
        metric.print_metrics()

    def test_prior_prob_a2kb_typed(self):
        print 'Prior prob + type improvements, A2KB'
        metric = Metrics()
        for filename, values in msnbc_data.iteritems():
            for line_dict in values:
                true_uri = line_dict['true_uri']
                text = line_dict['text']

                e_type = line_dict['type']
                if e_type is not None:
                    uri = get_max_typed_uri(text, e_type, ner)
                else:
                    uri = get_max_uri(text)

                metric.evaluate(true_uri, uri)
        metric.print_metrics()


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
