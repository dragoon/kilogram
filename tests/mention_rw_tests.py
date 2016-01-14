import unittest
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.dataset.msnbc import DataSet as MSNBC
from kilogram.entity_linking import syntactic_subsumption

from kilogram.entity_linking.evaluation import Metrics
from kilogram.entity_linking.mention_rw import SemanticGraph
from kilogram.entity_linking.util.ml import Feature
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService
import kilogram

kilogram.DEBUG = False

NgramService.configure(hbase_host=('diufpc304', '9090'), subst_table='typogram')
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
ngram_predictor = NgramTypePredictor('typogram')


class TestEntityLinkingKBMSNBC(unittest.TestCase):
    msnbc_data = MSNBC('../extra/data/msnbc/texts/',
                        '../extra/data/msnbc/msnbc_truth.txt', ner)

    def test_d2kb(self):
        print 'MENTION-RW, D2KB'
        feature_file = open('features.txt', 'w')
        metric = Metrics()
        feature_file.write(Feature.header()+'\n')
        for datafile in self.msnbc_data.data:
            syntactic_subsumption(datafile.candidates)
            graph = SemanticGraph(datafile.candidates)
            #import pickle
            #pickle.dump(graph.G, open(datafile.filename+'.pcl', 'wb'))
            for candidate in datafile:
                candidate.init_context_types(ngram_predictor)
            graph.do_linking()
            for candidate in datafile:
                # D2KB condition
                if candidate.truth_data['uri'] is None:
                    continue
                uri = candidate.resolved_true_entity
                metric.evaluate(candidate.truth_data, uri)
        feature_file.close()
        metric.print_metrics()


if __name__ == '__main__':
    print('Test MENTION-RW Entity Linking')
    unittest.main()
