from __future__ import division
import codecs
from collections import defaultdict
from numpy.random import choice, rand
from kilogram import ListPacker

NUM_STEPS = 10**5
ALPHA = 0.85  # restart probability


def build_edges_map():
    edges_map = {}
    edges = codecs.open('/Users/dragoon/Downloads/edges.txt', 'r', 'utf-8')
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
            print j
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
    for uri in edges_map.iterkeys():
        weights = semantic_signature(uri, edges_map)
        out.write(uri + '\t' + ' '.join([x[0]+','+str(x[1]) for x in weights]) + '\n')
    out.close()
