from __future__ import division
from .. import NgramService, ListPacker
import nltk
from .densest_subgraph import SemanticGraph
from ..lang.tokenize import default_tokenize_func


PERCENTILE = 0.9

class CandidateEntity:
    uri_counts = None
    start_i = 0
    end_i = 0
    noun_index = 0
    cand_string = None
    true_entity = None

    def __init__(self, start_i, end_i, noun_index, cand_string):
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        self.noun_index = noun_index
        table = "wiki_anchor_ngrams"
        column = "ngram:value"
        res = NgramService.hbase_raw(table, cand_string, column)
        if res:
            self.uri_counts = {}
            # take Xs percentile to remove noisy candidates
            temp_candidates = self._parse_candidate(res)
            total_c = sum(zip(*temp_candidates)[1])
            cur_c = 0
            for uri, count in sorted(temp_candidates, key=lambda x: x[1], reverse=True):
                if cur_c/total_c > PERCENTILE:
                    break
                cur_c += count
                self.uri_counts[uri] = count
            # also remove all counts = 1
            for uri in self.uri_counts.keys():
                if self.uri_counts[uri] < 2:
                    del self.uri_counts[uri]

    @staticmethod
    def _parse_candidate(cand_string):
        candidates = ListPacker.unpack(cand_string)
        return [(uri, long(count)) for uri, count in candidates]

    def __len__(self):
        return len(self.uri_counts)

    def __repr__(self):
        return self.cand_string + ":" + str(self.true_entity)


def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """

    entity_indexes = set()
    cand_entities = []

    # TODO: join noun sequences?
    def join_nps(pos_tokens):
        new_tokens = [list(pos_tokens[0])]
        is_nn = pos_tokens[0][1].startswith('NN')
        for word, pos_tag in pos_tokens[1:]:
            if is_nn and pos_tag.startswith('NN'):
                new_tokens[-1][1] += ' ' + pos_tag
                new_tokens[-1][0] += ' ' + word
            else:
                new_tokens.append([word, pos_tag])
                is_nn = pos_tag.startswith('NN')
        return new_tokens

    pos_tokens = join_nps(pos_tokens)

    noun_indexes = [i for i, word_token in enumerate(pos_tokens) if word_token[1].startswith('NN')]
    words = zip(*pos_tokens)[0]
    for noun_index in noun_indexes:
        n = 1
        while True:
            start_i = max(0, noun_index+1-n)
            end_i = min(len(words), noun_index+n)
            # whether to continue to expand noun phrase
            prev_len = len(cand_entities)
            for n_i, ngram in enumerate(nltk.ngrams(words[start_i:end_i], n)):
                ngram = ' '.join(ngram)
                cand_entity = CandidateEntity(start_i+n_i, start_i+n_i+n, noun_index, ngram)
                # TODO: what to do with lower-case things?
                #if not cand_entity.cand_string[0].isupper():
                #    continue
                if cand_entity.uri_counts and (cand_entity.start_i, cand_entity.end_i) not in entity_indexes:
                    entity_indexes.add((cand_entity.start_i, cand_entity.end_i))
                    cand_entities.append(cand_entity)
                # no uris? then check if sub-ngrams possible
                elif n == 1 and len(ngram.split()) > 1:
                    # concatenated nouns => split
                    ngram = ngram.split()
                    for i in range(len(ngram)-1, 0, -1):
                        for subngram in nltk.ngrams(ngram, i):
                            cand_entity = CandidateEntity(start_i, start_i+1, noun_index, ' '.join(subngram))
                            if cand_entity.uri_counts:
                                cand_entities.append(cand_entity)
                        if len(cand_entities) > prev_len:
                            break
            if prev_len == len(cand_entities):
                # nothing was added => break
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
