"""
spark-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks.py "/user/michael/plain_wikipedia_pagelinks" "/user/roman/SOTA_EL/wikipedia_pagelinks"
pig -p table=wiki_pagelinks -p path=/user/roman/wikipedia_pagelinks ./hbase_upload_array.pig
"""
import sys
from pyspark import SparkContext

sc = SparkContext(appName="WikipediaPageLinks")

pagelinks_file = sc.textFile(sys.argv[1])
def map_pagelinks(line):
    try:
        uri, link = line.strip().split('\t')
    except:
        return None
    link = link[0].upper()+link[1:]
    return link, uri

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

pagelinks = pagelinks_file.map(map_pagelinks).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

pagelinks.map(printer).saveAsTextFile(sys.argv[2])
