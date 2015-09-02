from __future__ import division
from .. import NgramService, ListPacker
import nltk
from ..lang.tokenize import default_tokenize_func

__author__ = 'dragoon'


class CandidateEntity:
    candidates = None
    start_i = 0
    end_i = 0

    def __init__(self, start_i, end_i, cand_string):
        self.start_i = start_i
        self.end_i = end_i
        self.candidates = sorted(self._parse_candidate(cand_string), key=lambda x: x[1], reverse=True)

    @property
    def first_count(self):
        return self.candidates[0][1]

    @property
    def first_uri(self):
        return self.candidates[0][0]

    @property
    def first_popularity(self):
        return self.first_count/sum(zip(*self.candidates)[1])

    @staticmethod
    def _parse_candidate(cand_string):
        candidates = ListPacker.unpack(cand_string)
        return [(uri, long(count)) for uri, count in candidates]

    def __len__(self):
        return len(self.candidates)


def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """

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
                res = NgramService.hbase_raw(table, ngram, column)
                if res:
                    cand_entities.append(CandidateEntity(start_i, end_i, res))
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
        if len(candidate.candidates) == 1:
            # if bigger count
            if most_probable_candidate is None or most_probable_candidate.first_count > candidate.first_count:
                most_probable_candidate = candidate
    prev_popularity = 0
    if not most_probable_candidate:
        # take most probable, or fail
        for candidate in candidates:
            popularity = candidate.first_popularity
            if popularity > 0.5 and popularity > prev_popularity:
                most_probable_candidate = candidate
                prev_popularity = popularity
    if not most_probable_candidate:
        return []
    # resolve via semantic similarity
    linkings = []
    for candidate in candidates:
        best_uri = None
        prev_count = 0
        for candidate_uri in candidate.candidates:
            overlap_count = NgramService.get_ref_count(most_probable_candidate.first_uri, candidate_uri)
            if overlap_count > prev_count:
                best_uri = candidate_uri
                prev_count = overlap_count
        linkings.append((candidate.start_i, candidate.end_i, best_uri))
    return linkings
