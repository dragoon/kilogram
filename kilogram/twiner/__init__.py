from __future__ import division
import functools
import math
from .. import NgramService
from ..lang.tokenize import generate_possible_splits
from repoze.lru import lru_cache


class InvalidProbability(Exception):
    pass


def _split_segment(words, i):
    return [words[:i+1], words[i+1:]]


def scp(words):
    if len(words) == 1:
        return 2 * math.log(_prior_prob(words))
    else:
        sets = [_split_segment(words, i) for i in range(len(words) - 1)]
        sum_prob = sum([_prior_prob(x[0]) * _prior_prob(x[1]) for x in sets])
        denom = sum_prob/(len(words) - 1)
        numer = math.pow(_prior_prob(words), 2)
        if denom < numer:
            print 'PROBABILITY ERROR'
        try:
            return math.log(numer/denom)
        except:
            raise InvalidProbability


@lru_cache(maxsize=1024)
def _prior_prob(words):
    return NgramService.get_joint_prob(" ".join(words), min(3, len(words)))


@lru_cache(maxsize=1024)
def _wiki_prob(words):
    return NgramService.get_wiki_prob(" ".join(words))


def _sigmoid(value):
    return 2 / (1 + math.exp(-value))


def _len_preference(words):
    return 1 if len(words) == 1 else ((len(words) - 1) / len(words))


@lru_cache(maxsize=1024)
def stickiness(words):
    try:
        return _len_preference(words) * math.exp(_wiki_prob(words)) * _sigmoid(scp(words))
    except InvalidProbability:
        return 0


def sum_stickiness(word_lists):
    return sum([stickiness(tuple(x)) for x in word_lists])


def rank_segments(sentence):
    analysis = [(x, sum_stickiness(x)) for x in generate_possible_splits(get_phrase(sentence), 5)]
    analysis.sort(key=lambda x: x[1], reverse=True)
    return analysis


def get_phrase(sentence):
    return sentence.lower().split()
