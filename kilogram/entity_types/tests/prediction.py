from __future__ import division
import unittest
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram import NgramService
from kilogram.entity_types.prediction import NgramTypePredictor

import pkg_resources


class WikipediaUserCuratedTest(unittest.TestCase):

    def setUp(self):
        NgramService.configure(hbase_host=('diufpc301', 9090))

    def test_base(self):
        skipped = 0
        total_correct_index = []
        dbpedia_ontology = DBPediaOntology('dbpedia_2014.owl')
        predictor = NgramTypePredictor("ngram_types", dbpedia_ontology)
        with pkg_resources.resource_stream('kilogram', r'entity_types/tests/test.tsv') as test_file:
            for line in test_file:
                line = line.strip().split()
                correct_type = line[2]
                try:
                    predicted_types = predictor.predict_types_full(line)
                    correct_index = predicted_types.index(correct_type)
                except ValueError:
                    skipped += 1
                    correct_index = len(predicted_types) + 1
                except IndexError:
                    # means empty list, continue
                    continue
                total_correct_index.append(correct_index)
        print skipped
        self.assertAlmostEqual(sum(total_correct_index)/len(total_correct_index), 2.665, 2)
        self.assertEquals(len(total_correct_index), 1000)
