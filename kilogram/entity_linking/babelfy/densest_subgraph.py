from __future__ import division
from collections import defaultdict
import networkx as nx
from kilogram import NgramService
import zmq


class SemanticGraph:
    G = None
    candidates = None
    uri_fragment_counts = None

    @staticmethod
    def avg_deg(G):
        return 2*G.number_of_edges()/G.number_of_nodes()

    def __init__(self, candidates):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("ipc:///tmp/wikipedia_signatures")
        self.G = nx.DiGraph()
        self.candidates = candidates
        neighbors = {}

        for cand in candidates:
            for e in cand.entities:
                neighbors[e.uri] = NgramService.get_wiki_edge_weights(e.uri)
                # delete self
                try:
                    del neighbors[e.uri][e.uri]
                except KeyError:
                    pass

        for cand_i in candidates:
            """
            :type cand_i: CandidateEntity
            """
            for cand_j in candidates:
                # do not link same candidates
                if cand_i == cand_j:
                    continue
                # skip edges between candidates originating from the same noun
                if cand_j.noun_index is not None and cand_i.noun_index is not None \
                        and cand_j.noun_index == cand_i.noun_index:
                    continue
                for e_i in cand_i.entities:
                    for e_j in cand_j.entities:
                        if not self.G.has_edge(e_i.uri, e_j.uri):
                            weight = int(neighbors[e_i.uri].get(e_j.uri, 0))
                            if weight > 0:
                                self.G.add_edge(e_i.uri, e_j.uri, {'w': weight})
        self.uri_fragment_counts = defaultdict(lambda: 0)
        # TODO: do not prune if no nodes?
        if self.G.number_of_nodes() == 0:
            return
        for cand in candidates:
            for e in cand.entities:
                if self.G.has_node(e.uri):
                    self.uri_fragment_counts[e.uri] += 1

    def _calculate_scores(self, candidate):
        total = 0
        scores = {}
        # pre-compute numerators and denominator
        for e in candidate.entities:
            w = self.uri_fragment_counts[e.uri]/max(len(self.candidates)-1, 1)
            scores[e.uri] = (self.G.degree([e.uri], weight='w').get(e.uri) or 0)*w
            total += scores[e.uri]
        if total > 0:
            for uri in scores.keys():
                scores[uri] /= total
        return scores

    def do_iterative_removal(self):
        best_G = self.G.copy()
        while True:
            candidate = max(self.candidates, key=lambda x: len(x))
            if len(candidate) < 10:
                break
            scores = self._calculate_scores(candidate)
            current_nodes = [item for item in scores.items() if self.G.has_node(item[0])]
            if len(current_nodes) <= 10:
                break
            min_uri = min(current_nodes, key=lambda x: x[1])[0]

            self.G.remove_node(min_uri)
            if SemanticGraph.avg_deg(self.G) > SemanticGraph.avg_deg(best_G):
                candidate.entities = [e for e in candidate.entities if e.uri != min_uri]
                best_G = self.G.copy()
        self.G = best_G

    def do_linking(self):
        # link starting from max possible candidate, remove other candidates
        while True:
            scores = [(candidate, max(self._calculate_scores(candidate).items(), key=lambda x: x[1]))
                      for candidate in self.candidates
                      if candidate.entities and not candidate.resolved_true_entity]
            if not scores:
                break
            candidate, uri_score = max(scores, key=lambda x: x[1][1])
            if uri_score[1] > 0:
                candidate.resolved_true_entity = uri_score[0]
            else:
                # max is 0, break and resort to max prob
                break
            # delete other entities
            for e in candidate.entities:
                if e.uri != candidate.resolved_true_entity and self.G.has_node(e.uri):
                    self.G.remove_node(e.uri)

        # max prob fall-back
        for candidate in self.candidates:
            if candidate.resolved_true_entity:
                continue
            candidate.resolved_true_entity = candidate.get_max_uri()
