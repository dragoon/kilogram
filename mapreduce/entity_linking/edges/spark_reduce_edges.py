"""
spark-submit --num-executors 20 --executor-memory 5g --master yarn-client ./entity_linking/edges/spark_reduce_edges.py "/user/roman/SOTA_EL/edges.txt" "/user/roman/SOTA_EL/edges_graph"
"""
import sys
from pyspark import SparkContext


sc = SparkContext(appName="SparkReduceEdges")

lines = sc.textFile(sys.argv[1])

def splitline(line):
    vertices, num = line.strip().split('\t')
    v1, v2 = vertices.split('|--|')
    return v1, (v2, num)

def seqfunc(u, v):
    u[v[0]] = v[1]
    return u

def combfunc(u1, u2):
    u1.update(u2)
    return u1

edges = lines.map(splitline).aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x+","+str(y) for x, y in value[1].items()])

edges.map(printer).saveAsTextFile(sys.argv[2])
