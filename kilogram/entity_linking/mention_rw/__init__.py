from __future__ import division
import math
import numpy as np
import networkx as nx
from sklearn.preprocessing import normalize
from kilogram import NgramService


class Signature(object):
    vector = None
    mapping = None

    def __init__(self, vector, G, candidate_uris):
        """
        :type candidate_uris: set
        """
        self.vector = vector
        self.mapping = []
        for prob, uri in zip(vector, G.nodes()):
            if uri in candidate_uris:
                self.mapping.append((prob, uri))
        self.mapping.sort(reverse=True)

    def __repr__(self):
        return str(self.mapping[:10])


def _mention_uri(uri, mention):
    return mention.replace(' ', '_')+'|'+uri


def _candidate_filter(candidates):

    def string_similar(candidate_, topn=10):
        substring_similar = [e for e in candidate_.entities
                             if set(candidate_.cand_string.lower().split()).intersection(e.uri.lower().split('_'))]
        if len(substring_similar) >= topn:
            return substring_similar
        substring_similar2 = [e for e in candidate_.entities
                              if candidate_.cand_string in e.uri.replace('_', ' ')]
        substring_similar.extend(substring_similar2)
        return substring_similar[:topn]

    def top_prior(candidate_, topn=10):
        return sorted(candidate_.entities, key=lambda e: e.count, reverse=True)[:topn]

    for candidate in candidates:
        entities = top_prior(candidate)
        uris = set(e.uri for e in entities)
        entities.extend([e for e in string_similar(candidate) if e.uri not in uris])
        candidate.entities = entities


ALPHA = 0.15  # restart probability


class SemanticGraph:
    G = None
    candidates = None
    matrix = None
    # map candidate urls to indexes in the matrix
    index_map = None
    candidate_uris = None

    def __init__(self, candidates):
        self.G = nx.Graph()
        _candidate_filter(candidates)
        self.candidates = candidates
        neighbors = {}
        self.index_map = {}

        #self.candidate_uris1 = set()
        #for cand in candidates:
        #    self.candidate_uris1.add(cand.cand_string)

        self.candidate_uris = set()
        for cand in candidates:
            total = sum([e.count for e in cand.entities])
            for e in cand.entities:
                mention_uri = _mention_uri(e.uri, cand.cand_string)
                self.candidate_uris.add(mention_uri)
                neighbors[mention_uri] = NgramService.get_wiki_link_mention_cooccur(mention_uri)
                # delete self
                try:
                    del neighbors[mention_uri][mention_uri]
                except KeyError:
                    pass
                for neighbor, weight in neighbors[mention_uri].iteritems():
                    #if neighbor.split('|')[0] not in self.candidate_uris1:
                    #    continue
                    if self.G.has_edge(mention_uri, neighbor):
                        continue
                    try:
                        self.G.add_edge(mention_uri, neighbor, {'w': int(weight)})
                    # happens because of malformed links
                    except ValueError:
                        pass
                # always add candidates
                self.G.add_node(mention_uri, {'prior': e.count/total})

        # prune 1-degree edges except original candidates
        to_remove = set()
        for node, degree in self.G.degree_iter():
            if degree <= 1:
                to_remove.add(node)
        to_remove = to_remove.difference(self.candidate_uris)
        self.G.remove_nodes_from(to_remove)

        if self.G.number_of_nodes() > 0:
            self.matrix = nx.to_scipy_sparse_matrix(self.G, weight='w', dtype=np.float64)
            self.matrix = normalize(self.matrix, norm='l1', axis=1)
        for i, uri in enumerate(self.G.nodes()):
            self.index_map[uri] = i

    def _get_entity_teleport_v(self, i):
        teleport_vector = np.zeros((self.matrix.shape[0], 1), dtype=np.float64)
        teleport_vector[i] = 1-ALPHA
        return np.matrix(teleport_vector)

    def _get_doc_teleport_v(self):
        teleport_vector = np.zeros((self.matrix.shape[0], 1), dtype=np.float64)
        resolved = [self.index_map[_mention_uri(x.resolved_true_entity, x.cand_string)] for x in self.candidates
                    if x.resolved_true_entity is not None]
        if len(resolved) > 0:
            for i in resolved:
                teleport_vector[i] = 1-ALPHA
        else:
            # assign according to prior probabilities
            for candidate in self.candidates:
                total_uri_count = sum([e.count for e in candidate.entities], 1)
                for e in candidate.entities:
                    teleport_vector[self.index_map.get(_mention_uri(e.uri, candidate.cand_string))] = e.count/total_uri_count

        return np.matrix(teleport_vector)

    def _learn_eigenvector(self, teleport_vector):

        pi = np.matrix(np.zeros(teleport_vector.shape))
        prev_norm = 0

        for _ in range(10000):
            pi = self.matrix*pi*ALPHA + teleport_vector
            cur_norm = np.linalg.norm(pi)
            pi /= cur_norm
            if prev_norm and abs(cur_norm - prev_norm) < 0.00001:
                break
            prev_norm = cur_norm
        return np.ravel(pi/pi.sum())

    def doc_signature(self):
        """compute document signature"""
        return Signature(self._learn_eigenvector(self._get_doc_teleport_v()), self.G, self.candidate_uris)

    def compute_signature(self, mention_uri):
        sig = Signature(self._learn_eigenvector(self._get_entity_teleport_v(self.index_map[mention_uri])), self.G, self.candidate_uris)
        return sig

    def _zero_kl_score(self, p, q):
        """
        :type p: Signature
        :type q: Signature
        :return: Zero Kullback-Leiber divergence score
        """
        total = 0
        for p_i, q_i in zip(p.vector, q.vector):
            if q_i == 0:
                total += p_i*20
            elif p_i > 0:
                total += p_i*math.log(p_i/q_i)
        return total

    def do_linking(self):
        # link unambiguous first
        for candidate in self.candidates:
            if len(candidate.entities) == 1:
                candidate.resolved_true_entity = candidate.entities[0].uri
        for candidate in sorted(self.candidates, key=lambda x: len(x.entities)):
            if candidate.truth_data['uri'] is None:
                continue
            if not candidate.entities or candidate.resolved_true_entity:
                continue
            doc_sign = self.doc_signature()
            cand_scores = []
            for e in candidate.entities:
                e_sign = self.compute_signature(_mention_uri(e.uri, candidate.cand_string))
                # global similarity + local (prior prob)
                sem_sim = 1/self._zero_kl_score(e_sign, doc_sign)
                cand_scores.append((e.uri, sem_sim))
            max_uri, score = max(cand_scores, key=lambda x: x[1])
            candidate.resolved_true_entity = max_uri
            if candidate.resolved_true_entity != candidate.truth_data['uri']:
                print candidate, candidate.truth_data['uri']
