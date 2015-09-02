import unittest
from kilogram.entity_linking import _extract_candidates
from kilogram import NgramService


class TestEntityLinking(unittest.TestCase):
    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', '9090'))

    def test_extract_candidates(self):
        self.assertIsNotNone(_extract_candidates([("Obama", "NNP")]))
        self.assertEquals(len(_extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])), 2)


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
