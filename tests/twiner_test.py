import unittest
from kilogram.twiner import *


class TestTwiner(unittest.TestCase):
    def setUp(self):
        NgramService.configure(ngram_table="3grams", hbase_host=('diufpc301', '9090'))

    def test_main(self):
        self.assertSequenceEqual(rank_segments("I live in New York")[0][0],
                                 [['i', 'live', 'in'], ['new', 'york']])
        self.assertSequenceEqual(rank_segments("I live in London")[0][0],
                                 [['i', 'live', 'in'], ['london']])


if __name__ == '__main__':
    print('Test Twiner')
    unittest.main()
