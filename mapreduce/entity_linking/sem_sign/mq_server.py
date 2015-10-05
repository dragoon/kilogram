from __future__ import division
import codecs
import zmq
import numpy as np
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
        self.prob_matrix = (1-ALPHA)*csr_matrix((data, (col_ind, row_ind)), shape=(len(index_map), len(index_map)))

    def _learn_eigenvector(self, i):
        teleport_vector = np.zeros(self.prob_matrix.shape[0], dtype=np.float64)
        teleport_vector[i] = ALPHA

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

    def _select_max(self, vector, topk):
        topk_ind = np.argpartition(vector, -topk)[-topk:]
        counts = vector[topk_ind] * NUM_STEPS
        return [(self.uri_list[i], int(count)) for i, count in zip(topk_ind, counts)]

    def semsign(self, i, topk=1000):
        return self._select_max(self._learn_eigenvector(i), topk)

s = SemSeignature('/home/roman/notebooks/kilogram/mapreduce/edges.txt')


def uri_map(i):
    return ' '.join([x[0].encode('utf-8') + ','+ str(x[1]) for x in s.semsign(int(i))])


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("ipc:///tmp/wikipedia_signatures")

while True:
    #  Wait for next request from client
    uri = socket.recv().strip()
    socket.send(uri_map(uri))
