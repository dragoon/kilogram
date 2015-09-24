import codecs
import multiprocessing
import zmq

socket = None

def initializer():
    global socket
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc:///tmp/wikipedia_edges")

def uri_map(item):
    socket.send(item.encode('utf-8'))
    return socket.recv()

def items():
    for line in codecs.open('wikipedia_pagelinks.tsv', 'r', 'utf-8'):
        try:
            label, value = line.strip().split('\t')
        except:
            continue
        yield label


if __name__ == "__main__":
    out = codecs.open('edges.txt', 'w', 'utf-8')
    pool = multiprocessing.Pool(10, initializer)

    j = 0
    for res in pool.imap_unordered(uri_map, items()):
        if res:
            out.write(res.decode('utf-8')+'\n')
        if not j % 10000:
            print j
        j += 1

    out.close()
    pool.close()
