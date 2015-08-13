from __future__ import division
import numpy as np


def transition_prob_matrix(segments_list):
    matrix = np.zeros((len(segments_list), len(segments_list)))
    for i, segment_i in enumerate(segments_list):
        for j, segment_j in enumerate(segments_list[i+1:]):
            matrix[i][j] = matrix[j][i] = len(segment_i.tweet_set & segment_j.tweet_set) / \
                                          len(segment_i.tweet_set | segment_j.tweet_set)
    return matrix



class TweetSegmentBlock(object):
    segments = None
    i = None

    def __init__(self):
        self.segments = {}
        self.i = 0

    def feed_tweet_segments(self, segment_list):
        for segment in segment_list:
            if segment not in self.segments:
                self.segments[segment] = TweetSegment(segment)
            self.segments[segment].tweet_set.add(self.i)
        self.i += 1


class TweetSegment(object):
    tweet_set = None
    teleport_prob = None
    segment_str = None

    def __init__(self, segment_str):
        from . import _wiki_prob
        self.segment_str = segment_str
        self.teleport_prob = _wiki_prob(segment_str)
        self.tweet_set = set()

    def __unicode__(self):
        return self.segment_str

    def __str__(self):
        return self.segment_str

    def __repr__(self):
        return self.segment_str
