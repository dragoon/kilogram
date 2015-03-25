from __future__ import division
import unittest
from kilogram.dataset.dbpedia import get_dbpedia_type_hierarchy
from kilogram import NgramService
from kilogram.entity_types.prediction import predict_types

import pkg_resources


class WikipediaUserCuratedTest(unittest.TestCase):

    def setUp(self):
        NgramService.configure(hbase_host=('diufpc301', 9090))

    def test_base(self):
        total_correct_index = []
        dbpedia_type_hierarchy = get_dbpedia_type_hierarchy('dbpedia_2014.owl')
        with pkg_resources.resource_stream('kilogram', r'entity_types/tests/test.tsv') as test_file:
            for line in test_file:
                line = line.strip().split()
                correct_type = line[2]
                try:
                    predicted_types = predict_types(line, dbpedia_type_hierarchy)
                    correct_index = predicted_types.index(correct_type)
                except ValueError:
                    correct_index = len(predicted_types) + 1
                except IndexError:
                    # means empty list, continue
                    continue
                total_correct_index.append(correct_index)
        print len(total_correct_index)
        self.assertAlmostEqual(sum(total_correct_index)/len(total_correct_index), 3.98, 2)
        self.assertEquals(len(total_correct_index), 659)
