import codecs
import multiprocessing
import zmq

socket = None

items = []

def initializer():
    global socket
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc:///tmp/wikipedia_edges")

def uri_map(item):
    socket.send(item.encode('utf-8'))
    return socket.recv()

if __name__ == "__main__":
    for line in codecs.open('wikipedia_pagelinks.tsv', 'r', 'utf-8'):
        try:
            label, value = line.strip().split('\t')
        except:
            continue
        items.append(label)
    out = codecs.open('edges.txt', 'w', 'utf-8')
    pool = multiprocessing.Pool(10, initializer)

    j = 0
    for res in pool.imap_unordered(uri_map, items):
        if res:
            out.write(res+'\n')
        if not j % 10000:
            print j
        j += 1

    out.close()
    pool.close()
