from __future__ import division
from .. import NgramService, ListPacker
import nltk
from ..lang.tokenize import default_tokenize_func


class CandidateEntity:
    candidates = None
    start_i = 0
    end_i = 0
    cand_string = None

    def __init__(self, start_i, end_i, cand_string):
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        table = "wiki_anchor_ngrams"
        column = "ngram:value"
        res = NgramService.hbase_raw(table, cand_string, column)
        if res:
            self.candidates = sorted(self._parse_candidate(res), key=lambda x: x[1], reverse=True)

    @staticmethod
    def _parse_candidate(cand_string):
        candidates = ListPacker.unpack(cand_string)
        return [(uri, long(count)) for uri, count in candidates]

    def __len__(self):
        return len(self.candidates)

    def __repr__(self):
        return self.cand_string


def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """

    cand_entities = []
    noun_indexes = [i for i, word_token in enumerate(pos_tokens) if word_token[1].startswith('NN')]
    words = zip(*pos_tokens)[0]
    for noun_index in noun_indexes:
        n = 1
        while True:
            start_i = max(0, noun_index+1-n)
            end_i = min(len(words), noun_index+n)
            # whether to continue to expand noun phrase
            should_break = True
            for ngram in nltk.ngrams(words[start_i:end_i], n):
                cand_entity = CandidateEntity(start_i, end_i, ' '.join(ngram))
                if cand_entity.candidates:
                    cand_entities.append(cand_entity)
                    should_break = False
            if should_break:
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
            if most_probable_candidate is None or most_probable_candidate.first_count < candidate.first_count:
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
        best_uri = NgramService.get_ref_count(most_probable_candidate.first_uri, zip(*candidate.candidates)[0])
        if best_uri:
            linkings.append((candidate.start_i, candidate.end_i, best_uri))
    return linkings
