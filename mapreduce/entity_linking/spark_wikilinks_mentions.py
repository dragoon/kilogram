"""
spark-submit --master yarn-client --executor-memory 5g --num-executors 10 ./entity_linking/spark_wikilinks_mentions.py "/user/ded/wikilinks_mentions" "/user/roman/SOTA_EL/wikipedia_pagelinks_mentions"
pig -p table=wiki_pagelinks_mentions -p path=/user/roman/wikipedia_pagelinks_mentions ./hbase_upload_array.pig
"""
import sys
from pyspark import SparkContext

sc = SparkContext(appName="WikipediaPageLinksMentions")

pagelinks_file = sc.textFile(sys.argv[1])

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

def mapper(line):
    # line format: State_(polity)	state	Authority	authority
    line = line.split('\t')
    return line[1]+'|'+line[0], line[3]+'|'+line[2]

pagelinks = pagelinks_file.filter(lambda x: '(disambiguation)' not in x).map(mapper).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

pagelinks.map(printer).saveAsTextFile(sys.argv[2])
