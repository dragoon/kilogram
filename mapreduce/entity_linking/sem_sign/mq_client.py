import codecs
import multiprocessing
import zmq

socket = None

def initializer():
    global socket
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc:///tmp/wikipedia_signatures")

def uri_map(item):
    uri, i = item
    socket.send(str(i))
    return uri + '\t' + socket.recv().decode('utf-8')

def items():
    for i, line in enumerate(codecs.open('/home/roman/notebooks/kilogram/mapreduce/edges.txt', 'r', 'utf-8')):
        try:
            uri, value = line.strip().split('\t')
        except:
            continue
        yield uri, i


if __name__ == "__main__":
    out = codecs.open('edges.txt', 'w', 'utf-8')
    pool = multiprocessing.Pool(15, initializer)

    j = 0
    for res in pool.imap_unordered(uri_map, items()):
        if res:
            out.write(res+'\n')
        if not j % 10000:
            print j
        j += 1

    out.close()
    pool.close()
