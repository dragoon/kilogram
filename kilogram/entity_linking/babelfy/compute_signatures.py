from __future__ import division
import codecs
from collections import defaultdict
import numpy as np
from numpy.random import choice, rand
from scipy.sparse import csr_matrix
from kilogram import ListPacker

NUM_STEPS = 10**6
ALPHA = 0.85  # restart probability
MIN_PROB = 100/(10**6)


class SemSignature:
    prob_matrix = None
    index_map = None
    reverse_index_map = None

    def __init__(self, edges_file):
        self.index_map = {}
        self.reverse_index_map = {}
        edges = codecs.open(edges_file, 'r', 'utf-8')
        j = 0
        print 'Building index map...'
        for line in edges:
            try:
                uri = line.strip().split('\t')[0]
                self.index_map[uri] = j
                self.reverse_index_map[j] = uri
            except:
                j += 1
                continue
            if not j % 100000:
                print "Loading:", j
            j += 1
        edges.close()

        j = 0
        data = []
        row_ind = []
        col_ind = []
        print 'Building sparse probability matrix...'
        edges = codecs.open(edges_file, 'r', 'utf-8')
        for line in edges:
            try:
                uri, neighbors = line.strip().split('\t')
            except ValueError:
                j += 1
                continue
            values = [(x[0], int(x[1])) for x in ListPacker.unpack(neighbors) if x[0] in self.index_map]
            row_ind.extend([j]*len(values))
            col_ind.extend([self.index_map[x[0]] for x in values])
            total = sum(zip(*values)[1])

            data.extend([x[1]/total for x in values])
            if not j % 100000:
                print "Loading:", j
            j += 1
        print 'Finish loading data...'

        # transpose immediately
        shape = len(self.index_map)
        self.prob_matrix = (1-ALPHA)*csr_matrix((data, (col_ind, row_ind)), shape=(shape, shape))

    def _learn_eigenvector(self, i):
        teleport_vector = np.zeros(self.prob_matrix.shape[0], dtype=np.float64)
        teleport_vector[i] = ALPHA

        pi = np.random.rand(teleport_vector.shape[0])
        prev_norm = 0

        for _ in range(1000):
            pi = self.prob_matrix.dot(pi) + teleport_vector
            cur_norm = np.linalg.norm(pi)
            pi /= cur_norm
            if prev_norm and abs(cur_norm - prev_norm) < 0.00001:
                break
            prev_norm = cur_norm
        return pi

    def semsign(self, uri):
        i = self.index_map[uri]
        vector = self._learn_eigenvector(i)
        normalized_prob = (1.0 - vector[i]/sum(vector))*sum(vector)
        return [(self.reverse_index_map[j], int(x*NUM_STEPS/normalized_prob)) for j, x in enumerate(vector) if x/normalized_prob > MIN_PROB and j != i]


def build_edges_map():
    edges_map = {}
    edges = codecs.open('edges.txt', 'r', 'utf-8')
    j = 0
    for line in edges:
        try:
            uri, neighbors = line.strip().split('\t')
        except ValueError:
            continue
        values = [(x[0], int(x[1])) for x in ListPacker.unpack(neighbors)]
        total = sum(zip(*values)[1])
        edges_map[uri] = (zip(*values)[0], [x[1]/total for x in values])
        if not j % 10000:
            print "Loading:", j
        j += 1
    edges.close()
    return edges_map


def semantic_signature(orig_uri, edges_map):
    weights = defaultdict(lambda: 0)
    uri = orig_uri
    for i in range(NUM_STEPS):
        if rand() <= ALPHA:
            # restart
            uri = orig_uri
            weights[uri] += 1
        try:
            values, p = edges_map[uri]
        except KeyError:
            # force restart
            uri = orig_uri
            continue
        uri = choice(values, p=p)
        weights[uri] += 1
    return [x for x in weights.iteritems() if x[1] > 10]


if __name__ == "__main__":
    out = codecs.open('sem_signatures.txt', 'w', 'utf-8')
    edges_map = build_edges_map()
    j = 0
    for uri in edges_map.iterkeys():
        weights = semantic_signature(uri, edges_map)
        out.write(uri + '\t' + ' '.join([x[0]+','+str(x[1]) for x in weights]) + '\n')
        if not j % 10000:
            print "Writing:", j
        j += 1
    out.close()
