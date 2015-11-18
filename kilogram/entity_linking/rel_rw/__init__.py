from __future__ import division
import math
import numpy as np
import networkx as nx
from kilogram import NgramService


def _candidate_filter(candidates):

    def string_similar(candidate_, topn=10):
        substring_similar = [x for x in candidate_.uri_counts.items()
                             if set(candidate_.cand_string.lower().split()).intersection(x[0].lower().split('_'))]
        if len(substring_similar) >= topn:
            return substring_similar
        substring_similar2 = [x for x in candidate_.uri_counts.items()
                              if candidate_.cand_string in x[0].replace('_', ' ')]
        substring_similar.extend(substring_similar2)
        return substring_similar[:topn]

    def top_prior(candidate_, topn=10):
        return sorted(candidate_.uri_counts.items(), key=lambda x: x[1], reverse=True)[:topn]

    for candidate in candidates:
        candidate.uri_counts = dict(top_prior(candidate) + string_similar(candidate))


ALPHA = 0.85  # restart probability


class SemanticGraph:
    G = None
    candidates = None
    matrix = None
    # map candidate urls to indexes in the matrix
    index_map = None

    def __init__(self, candidates):
        self.G = nx.Graph()
        _candidate_filter(candidates)
        self.candidates = candidates
        neighbors = {}
        self.index_map = {}

        candidate_uris = set()
        for cand in candidates:
            for uri in cand.uri_counts.keys():
                candidate_uris.add(uri)
                neighbors[uri] = NgramService.get_wiki_edge_weights(uri)
                # delete self
                try:
                    del neighbors[uri][uri]
                except KeyError:
                    pass

        for cand in candidates:
            """
            :type cand: CandidateEntity
            """
            for uri in cand.uri_counts.keys():
                for neighbor, weight in neighbors[uri].iteritems():
                    if self.G.has_edge(uri, neighbor):
                        continue
                    self.G.add_edge(uri, neighbor, {'w': weight})
                # always add candidates
                self.G.add_node(uri)

        # prune 1-degree edges except original candidates
        to_remove = set()
        for node, degree in self.G.degree_iter():
            if degree <= 1:
                to_remove.add(node)
        to_remove = to_remove.difference(candidate_uris)
        self.G.remove_nodes_from(to_remove)
        self.matrix = nx.to_scipy_sparse_matrix(self.G, weight='w', dtype=np.float32)
        for i, uri in enumerate(self.G.nodes()):
            self.index_map[uri] = i

    def _get_entity_teleport_v(self, i):
        teleport_vector = np.zeros(self.matrix.shape[0], dtype=np.float64)
        teleport_vector[i] = 1-ALPHA
        return teleport_vector

    def _get_doc_teleport_v(self):
        teleport_vector = np.zeros(self.matrix.shape[0], dtype=np.float64)
        resolved = [self.index_map[x.resolved_true_entity] for x in self.candidates
                    if x.resolved_true_entity is not None]
        if len(resolved) > 0:
            for i in resolved:
                teleport_vector[i] = 1
        else:
            # assign uniformly
            for i in range(teleport_vector):
                teleport_vector[i] = 1./self.matrix.shape[0]

        return teleport_vector

    def _learn_eigenvector(self, teleport_vector):

        pi = np.random.rand(teleport_vector.shape[0])
        prev_norm = 0

        for _ in range(1000):
            pi = self.matrix.dot(pi)*ALPHA + teleport_vector
            cur_norm = np.linalg.norm(pi)
            pi /= cur_norm
            if prev_norm and abs(cur_norm - prev_norm) < 0.00001:
                break
            prev_norm = cur_norm
        return pi/pi.sum()

    def doc_signature(self):
        """compute document signature"""
        return self._learn_eigenvector(self._get_doc_teleport_v())

    def compute_signatures(self, candidate):
        signatures = []
        for uri in candidate.uri_counts.keys():
            signatures.append((uri, self._learn_eigenvector(self._get_entity_teleport_v(self.index_map[uri]))))
        return signatures

    def _zero_kl_score(self, p, q):
        total = 0
        for p_i, q_i in zip(p, q):
            if q_i == 0:
                total += p_i*20
            elif p_i > 0:
                total += p_i*math.log(p_i/q_i)
        return total

    def do_linking(self):
        # link unambiguous first
        for candidate in self.candidates:
            if len(candidate.uri_counts) == 1:
                candidate.resolved_true_entity = candidate.uri_counts.items()[0][0]
        for candidate in sorted(self.candidates, key=lambda x: len(x.uri_counts)):
            if not candidate.uri_counts or candidate.resolved_true_entity:
                continue
            doc_sign = self.doc_signature()
            total_uri_count = sum([x for x in candidate.uri_counts.values()], 1)
            e_signatures = self.compute_signatures(candidate)
            cand_scores = []
            for uri, e_sign in e_signatures:
                # global similarity + local (prior prob)
                sem_sim = 1/self._zero_kl_score(e_sign, doc_sign) *\
                          candidate.uri_counts[uri]/total_uri_count
                cand_scores.append((uri, sem_sim))
            max_uri, score = max(cand_scores, key=lambda x: x[1])
            candidate.resolved_true_entity = max_uri

