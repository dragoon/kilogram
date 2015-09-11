"""
spark-submit --master yarn-client --executor-memory 5g --num-executors 20 ./entity_linking/spark_edge_weights.py "/user/roman/wikipedia_pagelinks" "/user/roman/wikipedia_edge_weights"
"""
import sys
from pyspark import SparkContext
from kilogram import ListPacker

sc = SparkContext(appName="WikipediaEdgeWeights")

pagelinks = sc.textFile(sys.argv[1]).map(lambda line: line.strip().split('\t'))

def triangles(elem):
    vertex1, vertex2 = elem
    entity1, related1 = vertex1
    entity2, related2 = vertex1
    related1 = dict(ListPacker.unpack(related1))
    related2 = dict(ListPacker.unpack(related2))
    common = set(related1.keys()).intersection(related2.keys())
    count = 0
    for elem in common:
        count += int(related1[elem])
    return entity1+','+entity2+'\t'+str(count)


def filter_triangles(elem):
    if elem[0][0] == elem[1][0]:
        return False
    vertex1, vertex2 = elem
    entity1, related1 = vertex1
    entity2, related2 = vertex1
    related1 = dict(ListPacker.unpack(related1))
    related2 = dict(ListPacker.unpack(related2))
    common = len(set(related1.keys()).intersection(related2.keys()))
    if common == 0:
        return False
    return True


selfjoin = pagelinks.cartesian(pagelinks).filter(filter_triangles).map(triangles)

selfjoin.saveAsTextFile(sys.argv[2])
