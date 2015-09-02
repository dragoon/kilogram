import unittest
from kilogram.entity_linking import _extract_candidates
from kilogram import NgramService


class TestEntityLinking(unittest.TestCase):
    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', '9090'))

    def test_extract_candidates(self):
        print _extract_candidates(u"Obama")



if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
