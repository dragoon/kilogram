from __future__ import division
import unittest
from kilogram import NgramService
from kilogram.entity_types.prediction import predict_types

import pkg_resources


class WikipediaUserCuratedTest(unittest.TestCase):

    def setUp(self):
        NgramService.configure(hbase_host=('diufpc301', 9090))

    def test_base(self):
        total_correct_index = []
        with pkg_resources.resource_stream('kilogram', r'entity_types/tests/test.tsv') as test_file:
            for line in test_file:
                line = line.strip().split()
                correct_type = line[2]
                try:
                    predicted_types = zip(*predict_types(line))[0]
                    correct_index = predicted_types.index(correct_type)
                except ValueError:
                    continue
                except IndexError:
                    correct_index = len(predicted_types) + 1
                total_correct_index.append(correct_index)
        self.assertAlmostEqual(sum(total_correct_index)/len(total_correct_index), 11.5, 1)
        self.assertEquals(len(total_correct_index), 994)
