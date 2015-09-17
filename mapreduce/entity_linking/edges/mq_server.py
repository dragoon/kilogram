import zmq
from kilogram import ListPacker

index_map = {}
values = []
index = 0

for line in open('labels.txt'):
    index_map[line.strip()] = index
    index += 1

for j, line in enumerate(open('wikipedia_pagelinks.tsv')):
    label, value = line.strip().split('\t')
    values.append(zip(*ListPacker.unpack(value))[0])
    if not j % 10000:
        print j
    j += 1

def uri_map(item):
    res = []
    uri, i = item.split('|--|')
    i = int(i)
    direct_neighbors = set(values[i])
    for neighbor in direct_neighbors:
        neighbor_uris = values[index_map[neighbor]]
        count = len(direct_neighbors.intersection(neighbor_uris))
        if count > 0:
            res.append(neighbor+','+str(count))

    return uri+'\t'+' '.join(res)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("ipc:///tmp/wikipedia_edges")

while True:
    #  Wait for next request from client
    uri = socket.recv().strip()
    socket.send(uri_map(uri))
