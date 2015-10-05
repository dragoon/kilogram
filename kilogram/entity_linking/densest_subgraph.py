from __future__ import division
from collections import defaultdict
import networkx as nx
from kilogram import ListPacker
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
            for uri in cand.candidates.keys():
                socket.send(uri.encode('utf-8'))
                neighbors[uri] = dict(ListPacker.unpack(socket.recv().decode('utf-8')))
                # neighbors[uri] = NgramService.get_wiki_edge_weights(uri)

        for i, cand_i in enumerate(candidates):
            """
            :type cand_i: CandidateEntity
            """
            for j, cand_j in enumerate(candidates):
                if i != j:
                    for uri_i in cand_i.candidates.keys():
                        for uri_j in cand_j.candidates.keys():
                            if not self.G.has_edge(uri_i, uri_j):
                                weight = int(neighbors[uri_i].get(uri_j, 0))
                                if weight > 0:
                                    self.G.add_edge(uri_i, uri_j, {'w': weight})
        self.uri_fragment_counts = defaultdict(lambda: 0)
        for cand in candidates:
            # immediately prune nodes without connections
            for uri in cand.candidates.keys():
                self.uri_fragment_counts[uri] += 1

    def _calculate_scores(self, candidate):
        total = 0
        scores = {}
        # pre-compute numerators and denominator
        for uri in candidate.candidates.keys():
            w = self.uri_fragment_counts[uri]/(len(self.candidates)-1)
            scores[uri] = (self.G.degree(uri) or 0)*w
            total += scores[uri]
        if total > 0:
            for uri in scores.keys():
                scores[uri] /= total
        return scores

    def do_iterative_removal(self):
        best_G = self.G.copy()
        while True:
            candidate = max(self.candidates, key=lambda x: len(x.candidates))
            if len(candidate.candidates) < 10:
                break
            scores = self._calculate_scores(candidate)
            current_nodes = [item for item in scores.items() if self.G.has_node(item[0])]
            if len(current_nodes) <= 10:
                break
            min_uri = min(current_nodes, key=lambda x: x[1])[0]

            self.G.remove_node(min_uri)
            if SemanticGraph.avg_deg(self.G) > SemanticGraph.avg_deg(best_G):
                del candidate.candidates[min_uri]
                best_G = self.G.copy()
        self.G = best_G

    def do_linking(self):
        for candidate in self.candidates:
            scores = self._calculate_scores(candidate)
            max_uri, score = max(scores.items(), key=lambda x: x[1])
            if score > 0.8:
                candidate.true_entity = max_uri

