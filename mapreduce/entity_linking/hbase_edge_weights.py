import codecs
from kilogram import ListPacker
from kilogram import NgramService

NgramService.configure(hbase_host=('diufpc304', 9090))
scanner = NgramService.h_client.scannerOpen("wiki_pagelinks", "", ["ngram"], None)

out = codecs.open('edges.txt', 'w', 'utf-8')
i = 0
while True:
    row = NgramService.h_client.scannerGet(scanner)[0]
    uri = row.row
    related_uris = set(zip(*ListPacker.unpack(row.columns['ngram:value'].value))[0])
    for related_uri in related_uris:
        count = len(set(zip(*NgramService.get_related_uris(related_uri))[0]).intersection(related_uris))
        out.write(uri+","+related_uri+'\t'+str(count))
    if not i % 10000:
        print i
    i += 1
out.close()
