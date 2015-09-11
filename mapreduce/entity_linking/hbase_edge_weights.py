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
    results = NgramService.h_client.getRows("wiki_pagelinks", list(related_uris), None)
    for related_uri, result in zip(related_uris, results):
        value = result.columns["ngram:value"].value
        count = len(set(zip(*ListPacker.unpack(value))[0]).intersection(related_uris))
        if count > 0:
            out.write(uri+","+related_uri+'\t'+str(count) + '\n')
    if not i % 10000:
        print i
    i += 1
out.close()
