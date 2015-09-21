"""
spark-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks.py "/user/roman/SOTA_EL/wikipedia_pagelinks"
pig -p table=wiki_pagelinks -p path=/user/roman/wikipedia_pagelinks ./hbase_upload_array.pig
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
    canon_uri = canon_uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return None
    return uri, canon_uri

dbp_redirects = dict(dbp_redirects_file.map(map_redirects).collect())


dbp_pagelinks_file = sc.textFile("/user/michael/wikilinks_20150602")
def map_pagelinks(line):
    uri, link = line.strip().split('\t')
    link = link[0].upper()+link[1:]
    return uri, dbp_redirects.get(link, link)

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

dbp_pagelinks = dbp_pagelinks_file.flatMap(map_pagelinks).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

dbp_pagelinks.map(printer).saveAsTextFile(sys.argv[1])
