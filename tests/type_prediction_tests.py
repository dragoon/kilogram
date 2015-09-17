import unittest
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService


class TestTyping(unittest.TestCase):
    ngram_predictor = None

    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', 9090), subst_table='typogram')
        dbpedia_ontology = DBPediaOntology('fixtures/dbpedia_2015-04.owl')
        self.ngram_predictor = NgramTypePredictor('typogram', dbpedia_ontology)

    def test_main(self):
        print self.ngram_predictor.predict_types("I went NONE NONE NONE".split())
