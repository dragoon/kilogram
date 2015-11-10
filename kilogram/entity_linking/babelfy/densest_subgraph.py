from __future__ import division
from collections import defaultdict
import networkx as nx
from entity_linking.priorprob import get_max_uri
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
            for uri in cand.uri_counts.keys():
                #socket.send(uri.encode('utf-8'))
                #neighbors[uri] = dict(ListPacker.unpack(socket.recv().decode('utf-8')))
                neighbors[uri] = NgramService.get_wiki_edge_weights(uri)

        for cand_i in candidates:
            """
            :type cand_i: CandidateEntity
            """
            for cand_j in candidates:
                # skip edges between candidates originating from the same noun
                if cand_j.noun_index == cand_i.noun_index:
                    continue
                for uri_i in cand_i.uri_counts.keys():
                    for uri_j in cand_j.uri_counts.keys():
                        if not self.G.has_edge(uri_i, uri_j):
                            weight = int(neighbors[uri_i].get(uri_j, 0))
                            if weight > 0:
                                self.G.add_edge(uri_i, uri_j, {'w': weight})
        self.uri_fragment_counts = defaultdict(lambda: 0)
        # TODO: do not prune if no nodes?
        if self.G.number_of_nodes() == 0:
            return
        for cand in candidates:
            # immediately prune nodes without connections
            for uri in cand.uri_counts.keys():
                if self.G.has_node(uri):
                    self.uri_fragment_counts[uri] += 1
                else:
                    del cand.uri_counts[uri]

    def _calculate_scores(self, candidate):
        total = 0
        scores = {}
        # pre-compute numerators and denominator
        for uri in candidate.uri_counts.keys():
            w = self.uri_fragment_counts[uri]/max(len(self.candidates)-1, 1)
            scores[uri] = (self.G.degree([uri], weight='w').get(uri) or 0)*w
            total += scores[uri]
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
                del candidate.uri_counts[min_uri]
                best_G = self.G.copy()
        self.G = best_G

    def do_linking(self):
        # link starting from max possible candidate, remove other candidates
        while True:
            scores = [(candidate, max(self._calculate_scores(candidate).items(), key=lambda x: x[1]))
                      for candidate in self.candidates
                      if candidate.uri_counts and not candidate.true_entity]
            if not scores:
                break
            candidate, uri_score = max(scores, key=lambda x: x[1][1])
            if uri_score[1] > 0:
                candidate.true_entity = uri_score[0]
            else:
                # max is 0, break and resort to max prob
                break
            # delete other entities
            for uri in candidate.uri_counts.keys():
                if uri != candidate.true_entity and self.G.has_node(uri):
                    self.G.remove_node(uri)

        # max prob
        for candidate in self.candidates:
            if candidate.true_entity:
                continue
            true_entity = get_max_uri(candidate.cand_string)
            # makes sure max probable uri is not removed by type pruning
            if true_entity in candidate.uri_counts:
                candidate.true_entity = true_entity
