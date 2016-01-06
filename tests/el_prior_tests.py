import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import DataSet
from entity_linking import syntactic_subsumption
from entity_linking.evaluation import Metrics
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

    def test_d2kb(self):
        print 'Prior prob, D2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            for candidate in datafile:
                # D2KB condition
                if candidate.truth_data['uri'] is None:
                    continue
                metric.evaluate(candidate.truth_data, candidate.get_max_uri())
        metric.print_metrics()
        print

    def test_a2kb(self):
        print 'Prior prob, A2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            for candidate in datafile:
                uri = None
                # A2KB condition
                if candidate.context is not None:
                    uri = candidate.get_max_uri()
                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()
        print

    def test_d2kb_typed(self):
        print 'Prior prob + type improvements, D2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            for candidate in datafile:
                # D2KB
                if candidate.truth_data['uri'] is None:
                    continue
                if candidate.type is not None:
                    uri = candidate.get_max_typed_uri()
                else:
                    uri = candidate.get_max_uri()

                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()
        print

    def test_a2kb_typed(self):
        print 'Prior prob + type improvements, A2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            for candidate in datafile:
                uri = None
                # A2KB condition
                if candidate.context is not None:
                    if candidate.e_type is not None:
                        uri = candidate.get_max_typed_uri(ner)
                    else:
                        uri = candidate.get_max_uri()

                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()
        print


if __name__ == '__main__':
    print('Test PriorProb Entity Linking')
    unittest.main()
