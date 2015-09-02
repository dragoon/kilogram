"""
spark-submit --master yarn-client ./entity_linking/spark_reduce_wikilinks.py "/user/roman/wikipedia_pagelinks"
"""
import sys
from pyspark import SparkContext

sc = SparkContext(appName="WikipediaPageLinks")


dbp_pagelinks_file = sc.textFile("/user/roman/page-links_en.nt.bz2")
def map_pagelinks(line):
    try:
        uri, _, link, _ = line.split()
    except:
        return [None]
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return [None]
    link = link.replace('<http://dbpedia.org/resource/', '')[:-1]
    if ':' in link:
        return [None]
    return [(uri, link), (link, uri)]

def seqfunc(u, v):
    if v in u:
        u[v] += 1
    else:
        u[v] = 1
    return u

def combfunc(u1, u2):
    for k, v in u2.iteritems():
        if k in u1:
            u1[k] += v
        else:
            u1[k] = v
    return u1

dbp_pagelinks = dbp_pagelinks_file.flatMap(map_pagelinks).filter(lambda x: x is not None).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

dbp_pagelinks.map(printer).saveAsTextFile(sys.argv[1])
