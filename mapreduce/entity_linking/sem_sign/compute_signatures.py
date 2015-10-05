from __future__ import division
import codecs
from collections import defaultdict
import numpy as np
from numpy.random import choice, rand
from scipy.sparse import csr_matrix
from kilogram import ListPacker

NUM_STEPS = 10**5
ALPHA = 0.85  # restart probability


class SemSeignature:
    prob_matrix = None
    uri_list = []

    def __init__(self, edges_file):
        index_map = {}
        edges = codecs.open(edges_file, 'r', 'utf-8')
        j = 0
        print 'Building index map...'
        for line in edges:
            try:
                uri = line.strip().split('\t')[0]
                index_map[uri] = j
                self.uri_list.append(uri)
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
            values = [(x[0], int(x[1])) for x in ListPacker.unpack(neighbors) if x[0] in index_map]
            row_ind.extend([j]*len(values))
            col_ind.extend([index_map[x[0]] for x in values])
            total = sum(zip(*values)[1])

            data.extend([x[1]/total for x in values])
            if not j % 100000:
                print "Loading:", j
            j += 1
        print 'Finish loading data...'

        # transpose immediately
        self.prob_marix = ALPHA*csr_matrix((data, (col_ind, row_ind)), shape=(len(index_map), len(index_map)))

    def _learn_eigenvector(self, i):
        teleport_vector = np.zeros(self.prob_matrix.shape[0], dtype=np.float64)
        teleport_vector[i] = 1 - ALPHA

        pi = np.random.rand(teleport_vector.shape[0])
        prev_norm = 0

        for _ in range(NUM_STEPS):
            pi = self.prob_matrix.dot(pi) + teleport_vector
            cur_norm = np.linalg.norm(pi)
            pi /= cur_norm
            if prev_norm and abs(cur_norm - prev_norm) < 0.00001:
                break
            prev_norm = cur_norm
        return pi

    def _select_max(self, vector, topk=1000):
        topk_ind = np.argpartition(vector, -topk)[-topk:]
        counts = vector[topk_ind] * NUM_STEPS
        return [(self.uri_list[i], int(count)) for i, count in zip(topk_ind, counts)]

    def semsign(self, i):
        return self._select_max(self._learn_eigenvector(i))


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
