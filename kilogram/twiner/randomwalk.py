from __future__ import division
from collections import OrderedDict
import math
import numpy as np


class TweetSegmentBlock(object):
    segments = None
    i = None
    transition_matrix = None
    teleport_vector = None

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
        self.teleport_vector = np.zeros(len(self.segments))
        matrix = np.zeros((len(self.segments), len(self.segments)))
        for i, segment_i in enumerate(self.segments.values()):
            total_teleport_prob = segment_i.teleport_prob
            total_weight = 0
            for j, segment_j in enumerate(self.segments.values()):
                if i == j:
                    continue
                intersec_len = len(segment_i.tweet_set & segment_j.tweet_set)
                if intersec_len > 0:
                    matrix[i][j] = intersec_len / len(segment_i.tweet_set | segment_j.tweet_set)
                    total_teleport_prob += segment_j.teleport_prob
                total_weight += matrix[i][j]
            for j in range(len(self.segments)):
                matrix[i][j] /= total_weight
            if segment_i.teleport_prob:
                self.teleport_vector[i] = segment_i.teleport_prob / (total_teleport_prob)
        self.transition_matrix = matrix


class TweetSegment(object):
    tweet_set = None
    teleport_prob = None
    segment_str = None

    def __init__(self, segment_str):
        from . import _wiki_prob
        self.segment_str = segment_str
        self.teleport_prob = math.exp(_wiki_prob(tuple(segment_str.split())))
        self.tweet_set = set()

    def __unicode__(self):
        return self.segment_str

    def __str__(self):
        return self.segment_str

    def __repr__(self):
        return self.segment_str
