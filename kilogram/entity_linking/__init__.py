from __future__ import division
from .. import NgramService, ListPacker
import nltk
from ..lang.tokenize import default_tokenize_func

__author__ = 'dragoon'


def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """
    def parse_candidate(cand_string):
        candidates = ListPacker.unpack(cand_string)
        return [(uri, long(count)) for uri, count in candidates]

    table = "wiki_anchor_ngrams"
    column = "ngram:value"
    cand_entities = []
    noun_indexes = [i for i, word_token in enumerate(pos_tokens) if word_token[1].startswith('NN')]
    words = zip(*pos_tokens)[0]
    for noun_index in noun_indexes:
        n = 1
        while True:
            start_i = max(0, noun_index+1-n)
            end_i = min(len(words), noun_index+n)
            should_break = []
            for ngram in nltk.ngrams(words[start_i:end_i], n):
                ngram = ' '.join(ngram)
                res = parse_candidate(NgramService.hbase_raw(table, ngram, column))
                if res:
                    cand_entities.append((start_i, end_i, res))
                    should_break.append(False)
                else:
                    should_break.append(True)
            if all(should_break):
                break
            if start_i == 0 and end_i == len(words)-noun_index:
                break
            n += 1
    return cand_entities


def link(sentence):
    tokens = default_tokenize_func(sentence)
    pos_tokens = nltk.pos_tag(tokens)
    candidates = _extract_candidates(pos_tokens)
    most_probable_candidate = None
    for candidate in candidates:
        if len(candidate[2]) == 1:
            # if bigger count
            if most_probable_candidate is None or most_probable_candidate[2][0][1] > candidate[2][0][1]:
                most_probable_candidate = candidate
    prev_popularity = 0
    if not most_probable_candidate:
        #take most probable, or fail
        for candidate in candidates:
            uri, popularity = sorted(candidate[2], key=lambda x: x[1], reverse=True)[0]
            popularity /= sum(zip(*candidate[2])[1])
            if popularity > 0.5 and popularity > prev_popularity:
                most_probable_candidate = candidate
                prev_popularity = popularity
    if not most_probable_candidate:
        return []
    return [most_probable_candidate]
