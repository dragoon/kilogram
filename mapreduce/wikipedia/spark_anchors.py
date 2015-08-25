"""
spark-submit --master yarn-client ./wikipedia/spark_anchors.py "/data/wikipedia2015_plaintext_annotated" "/user/roman/wikipedia_anchors_orig"
"""
import sys
from pyspark import SparkContext
import re
from collections import defaultdict

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
            result.append((anchor_text, (uri, 1)))
    return result

anchor_counts = lines.flatMap(unpack_anchors)

def seqfunc(u, v):
    if v[0] in u:
        u[v[0]] += v[1]
    else:
        u[v[0]] = v[1]
    return u

def combfunc(u1, u2):
    for k, v in u2.iteritems():
        u1[k] += v
    return u1

anchor_counts_agg = anchor_counts.aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

anchor_counts_agg.map(printer).saveAsTextFile(sys.argv[2])
