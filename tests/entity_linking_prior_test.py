import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import DataSet
from entity_linking.evaluation import Metrics
from entity_linking.priorprob import get_max_uri, get_max_typed_uri
from kilogram import NgramService
import kilogram

NgramService.configure(hbase_host=('diufpc304', '9090'))
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
msnbc_data = DataSet('../extra/data/msnbc/texts/',
                        '../extra/data/msnbc/msnbc_truth.txt', ner)


class TestEntityLinking(unittest.TestCase):

    def test_prior_prob_d2kb(self):
        print 'Prior prob, D2KB'
        metric = Metrics()
        for filename, values in msnbc_data.data.iteritems():
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
        for filename, values in msnbc_data.data.iteritems():
            for line_dict in values:
                true_uri = line_dict['true_uri']
                text = line_dict['text']
                uri = get_max_uri(text)

                metric.evaluate(true_uri, uri)
        metric.print_metrics()

    def test_prior_prob_d2kb_typed(self):
        print 'Prior prob + type improvements, D2KB'
        metric = Metrics()
        for filename, values in msnbc_data.data.iteritems():
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
        for filename, values in msnbc_data.data.iteritems():
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
