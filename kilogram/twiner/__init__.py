from __future__ import division
import functools
import math
from .. import NgramService
from lang.tokenize import generate_possible_splits
from repoze.lru import lru_cache


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
        return math.log(numer/denom)


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
    return _len_preference(words) * math.exp(_wiki_prob(words)) * _sigmoid(scp(words))


def sum_stickiness(word_lists):
    return sum([stickiness(x) for x in word_lists])


def rank_segments(sentence):
    analysis = [sum_stickiness(x) for x in generate_possible_splits(get_phrase(sentence))]
    analysis.sort(key=lambda x: x[1])
    return analysis
