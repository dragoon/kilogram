"""
spark-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks.py "/user/roman/wikipedia_pagelinks"
"""
import sys
from pyspark import SparkContext

sc = SparkContext(appName="WikipediaPageLinks")

# Redirects to exclude redirect pages
dbp_redirects_file = sc.textFile("/user/roman/redirects_transitive_en.nt.bz2")
def map_redirects(line):
    try:
        uri, _, canon_uri, _ = line.split()
    except:
        return None
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return None
    return uri

dbp_redirects = set(dbp_redirects_file.map(map_redirects).collect())


dbp_pagelinks_file = sc.textFile("/user/roman/page-links_en.nt.bz2")
def map_pagelinks(line):
    uri, _, link, _ = line.split()
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    link = link.replace('<http://dbpedia.org/resource/', '')[:-1]
    return [(uri, link), (link, uri)]

def filter_pagelins(line):
    if '(disambiguation)' in line:
        return False
    try:
        uri, _, link, _ = line.split()
    except:
        return False
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    link = link.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return False
    if ':' in link:
        return False
    if uri in dbp_redirects:
        return False
    return True

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

dbp_pagelinks = dbp_pagelinks_file.filter(filter_pagelinks).flatMap(map_pagelinks).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

dbp_pagelinks.map(printer).saveAsTextFile(sys.argv[1])
