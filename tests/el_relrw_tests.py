import unittest
from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import DataSet
from entity_linking import syntactic_subsumption

from entity_linking.evaluation import Metrics
from entity_linking.rel_rw import SemanticGraph
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


class TestEntityLinkingKB(unittest.TestCase):

    def test_d2kb(self):
        print 'REL-RW, D2KB'
        metric = Metrics()
        for datafile in msnbc_data.data:
            #syntactic_subsumption(datafile.candidates)
            graph = SemanticGraph(datafile.candidates)
            graph.do_linking()
            for candidate in datafile:
                # D2KB condition
                if candidate.truth_data['uri'] is None:
                    continue
                uri = candidate.resolved_true_entity
                metric.evaluate(candidate.truth_data, uri)
        metric.print_metrics()


if __name__ == '__main__':
    print('Test REL-RW Entity Linking')
    unittest.main()
