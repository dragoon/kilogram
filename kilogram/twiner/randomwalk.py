from __future__ import division
from collections import OrderedDict
import numpy as np


class TweetSegmentBlock(object):
    segments = None
    i = None

    def __init__(self):
        self.segments = OrderedDict()
        self.i = 0

    def feed_tweet_segments(self, segment_list):
        for segment in segment_list:
            segment = ' '.join(segment)
            if segment not in self.segments:
                self.segments[segment] = TweetSegment(segment)
            self.segments[segment].tweet_set.add(self.i)
        self.i += 1

    def transition_prob_matrix(self):
        matrix = np.zeros((len(self.segments), len(self.segments)))
        for i, segment_i in enumerate(self.segments.values()):
            for j, segment_j in enumerate(self.segments.values()[i+1:]):
                intersec_len = len(segment_i.tweet_set & segment_j.tweet_set)
                if intersec_len > 0:
                    matrix[i][j+i+1] = matrix[j+i+1][i] = intersec_len / len(segment_i.tweet_set | segment_j.tweet_set)
        return matrix


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
