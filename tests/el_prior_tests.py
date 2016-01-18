import unittest

from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.dataset.entity_linking.msnbc import DataSet
from kilogram.entity_linking import syntactic_subsumption
from kilogram.entity_linking.evaluation import Metrics
from kilogram.entity_types.prediction import NgramTypePredictor
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

ngram_predictor = NgramTypePredictor('typogram')


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
                candidate.init_context_types(ngram_predictor)
            for candidate in datafile:
                # D2KB
                if candidate.truth_data['uri'] is None:
                    continue
                context_types_list = [c.context_types for c in datafile if c.context_types and c.cand_string == candidate.cand_string]
                if context_types_list:
                    uri = candidate.get_max_typed_uri(context_types_list)
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
                candidate.init_context_types(ngram_predictor)
            for candidate in datafile:
                uri = None
                # A2KB condition
                if candidate.context is not None:
                    context_types_list = [c.context_types for c in datafile if c.context_types and c.cand_string == candidate.cand_string]
                    if context_types_list:
                        uri = candidate.get_max_typed_uri(context_types_list)
                    else:
                        uri = candidate.get_max_uri()

                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()
        print


if __name__ == '__main__':
    print('Test PriorProb Entity Linking')
    unittest.main()
