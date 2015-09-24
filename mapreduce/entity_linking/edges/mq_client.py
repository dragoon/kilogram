import codecs
import multiprocessing
import zmq

socket = None

index_map = {}
index = 0

def initializer():
    global socket
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc:///tmp/wikipedia_edges")

def uri_map(item):
    socket.send(item[0]+'|--|'+str(item[1]))
    return socket.recv()

if __name__ == "__main__":
    for line in codecs.open('wikipedia_pagelinks.tsv', 'r', 'utf-8'):
        try:
            label, value = line.strip().split('\t')
        except:
            continue
        index_map[label] = index
        index += 1
    out = codecs.open('edges.txt', 'w', 'utf-8')
    pool = multiprocessing.Pool(10, initializer)

    j = 0
    for res in pool.imap_unordered(uri_map, index_map.iteritems()):
        if res:
            out.write(res+'\n')
        if not j % 10000:
            print j
        j += 1

    out.close()
    pool.close()
