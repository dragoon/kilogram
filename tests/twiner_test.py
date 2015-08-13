import unittest
from kilogram.twiner import *
from kilogram.twiner.randomwalk import TweetSegmentBlock


class TestTwiner(unittest.TestCase):
    def setUp(self):
        NgramService.configure(ngram_table="3grams", hbase_host=('diufpc301', '9090'))

    def test_main(self):
        self.assertSequenceEqual(rank_segments("I live in New York")[0][0],
                                 [['i', 'live', 'in'], ['new', 'york']])
        self.assertSequenceEqual(rank_segments("I live in London")[0][0],
                                 [['i', 'live', 'in'], ['london']])

    def test_random_walk(self):
        tsb = TweetSegmentBlock()
        tsb.feed_tweet_segments([['barack', 'obama'], ['michelle', 'obama']])
        tsb.feed_tweet_segments([['barack', 'obama'], ['john', 'kerry']])
        tsb.feed_tweet_segments([['president', 'putin'], ['john', 'kerry']])
        print tsb.segments.values()
        tsb.transition_prob_matrix()
        print tsb.transition_matrix
        print tsb.teleport_vector


if __name__ == '__main__':
    print('Test Twiner')
    unittest.main()
