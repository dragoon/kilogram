"""
spark-submit --executor-memory 5g --master yarn-client ./wikipedia/spark_anchors.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_anchors"
"""
import sys
from pyspark import SparkContext
import re

ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')


sc = SparkContext(appName="WikipediaAnchors")

lines = sc.textFile(sys.argv[1])

# Split each line into words
def unpack_anchors(line):
    result = []
    line = line.strip()
    for word in line.split():
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            anchor_text = match.group(2)
            anchor_text = anchor_text.replace('_', ' ')
            result.append((uri[0].upper()+uri[1:], anchor_text))
    return result

anchor_counts = lines.flatMap(unpack_anchors)


dbp_redirects_file = sc.textFile("/user/roman/redirects_transitive_en.nt.bz2")
def map_redirects(line):
    try:
        uri, _, canon_uri, _ = line.split()
    except:
        return [(None, None)]
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return [(None, None)]
    canon_uri = canon_uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    # return only redirect, we'll do left join later
    return uri, canon_uri

dbp_urls = dbp_redirects_file.map(map_redirects).distinct()
anchor_counts_join = anchor_counts.leftOuterJoin(dbp_urls).map(lambda x: x[1] if x[1][0] else (x[0], x[1][1]))


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

anchor_counts_agg = anchor_counts_join.aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

anchor_counts_agg.map(printer).saveAsTextFile(sys.argv[2])
