import unittest
from kilogram.twiner import *
from kilogram.twiner.randomwalk import TweetSegmentBlock


class TestTwiner(unittest.TestCase):
    def setUp(self):
        NgramService.configure(ngram_table="3grams", hbase_host=('diufpc301', '9090'))

    def test_main(self):
        self.assertSequenceEqual(rank_segments("I live in New York")[0][0],
                                 [['i'], ['live'], ['in'], ['new', 'york']])
        self.assertSequenceEqual(rank_segments("I live in London")[0][0],
                                 [['i'], ['live'], ['in'], ['london']])
        self.assertSequenceEqual(rank_segments("white house reveals obama summer reading list")[0][0],
                                 [['white', 'house'], ['reveals'], ['obama'], ['summer'], ['reading', 'list']])

    def test_splits(self):
        for line in open('fixtures/sample.txt'):
            try:
                if len(line.split()) > 10:
                    continue
                ranking = rank_segments(line)[0][0]
            except:
                continue
            print ranking

    def test_random_walk_existing_splits(self):
        tsb = TweetSegmentBlock()
        for line in open('fixtures/splits.txt'):
            ranking = eval(line.strip())
            tsb.feed_tweet_segments(ranking)
        print tsb.segments.values()
        tsb.transition_prob_matrix()
        print tsb.transition_matrix
        print tsb.teleport_vector
        eig_vec = tsb.learn_eigenvector(0.4)
        final_vec = eig_vec * tsb.teleport_vector
        print sorted(zip(list(tsb.teleport_vector), tsb.segments.values()), reverse=True)
        print sorted(zip(list(eig_vec), tsb.segments.values()), reverse=True)
        print sorted(zip(list(final_vec), tsb.segments.values()), reverse=True)


if __name__ == '__main__':
    print('Test Twiner')
    unittest.main()
