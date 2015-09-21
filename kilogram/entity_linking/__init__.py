from __future__ import division
from .. import NgramService, ListPacker
import nltk
from .densest_subgraph import SemanticGraph
from ..lang.tokenize import default_tokenize_func


class CandidateEntity:
    candidates = None
    start_i = 0
    end_i = 0
    cand_string = None
    true_entity = None

    def __init__(self, start_i, end_i, cand_string):
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        table = "wiki_anchor_ngrams"
        column = "ngram:value"
        res = NgramService.hbase_raw(table, cand_string, column)
        if res:
            self.candidates = dict(self._parse_candidate(res))

    @staticmethod
    def _parse_candidate(cand_string):
        candidates = ListPacker.unpack(cand_string)
        return [(uri, long(count)) for uri, count in candidates]

    def __len__(self):
        return len(self.candidates)

    def __repr__(self):
        return self.cand_string + ":" + self.true_entity


def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """

    entity_indexes = set()
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
            for n_i, ngram in enumerate(nltk.ngrams(words[start_i:end_i], n)):
                cand_entity = CandidateEntity(start_i+n_i*n, start_i+(n_i+1)*n, ' '.join(ngram))
                if cand_entity.candidates and (cand_entity.start_i, cand_entity.end_i) not in entity_indexes:
                    entity_indexes.add((cand_entity.start_i, cand_entity.end_i))
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
    graph = SemanticGraph(candidates)
    graph.do_iterative_removal()
    graph.do_linking()
    return candidates
