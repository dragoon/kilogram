from __future__ import division
import zmq
from kilogram.mapreduce.entity_linking.sem_sign.compute_signatures import SemSignature


s = SemSignature('/home/roman/notebooks/kilogram/mapreduce/edges.txt')

def uri_map(uri):
    uri = uri.decode('utf-8')
    return ' '.join([x[0].encode('utf-8') + ',' + str(x[1]) for x in s.semsign(uri)])


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("ipc:///tmp/wikipedia_signatures")

while True:
    #  Wait for next request from client
    uri = socket.recv().strip()
    socket.send(uri_map(uri))
