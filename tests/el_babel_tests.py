import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.entity_linking.msnbc import DataSet
from entity_linking import syntactic_subsumption
from entity_linking.babelfy import _extract_candidates, link, SemanticGraph
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

    def test_extract_candidates(self):
        self.assertIsNotNone(_extract_candidates([("Obama", "NNP")]))
        self.assertEquals(len(_extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])), 2)

    def test_entity_linking(self):
        print link("After his departure from Buffalo, Saban returned to coach college football teams including Miami, Army and UCF.")
        print link("Barack and Michelle visited us today.")
        print link("GitHub experienced a massive DDoS attack yesterday evening.")
        print link("Saban, previously a head coach of NFL's Miami, is now coaching Crimson Tide. "
                   "His achievements include leading LSU to the BCS National Championship once and Alabama three times.")


class TestEntityLinkingKB(unittest.TestCase):

    def test_a2kb(self):
        print 'Babelfy, A2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            graph = SemanticGraph(datafile.candidates)
            graph.do_iterative_removal()
            graph.do_linking()
            for candidate in datafile:
                uri = candidate.resolved_true_entity
                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()

    def test_a2kb_typed(self):
        print 'Babelfy, A2KB + Types'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            graph = SemanticGraph(datafile.candidates)
            graph.do_iterative_removal()
            graph.do_linking()
            for candidate in datafile:
                uri = candidate.resolved_true_entity
                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()

    def test_d2kb(self):
        print 'Babelfy, D2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            graph = SemanticGraph(datafile.candidates)
            graph.do_iterative_removal()
            graph.do_linking()
            for candidate in datafile:
                # D2KB condition
                if candidate.truth_data['uri'] is None:
                    continue
                uri = candidate.resolved_true_entity
                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()


if __name__ == '__main__':
    print('Test Babelfy Entity Linking')
    unittest.main()
