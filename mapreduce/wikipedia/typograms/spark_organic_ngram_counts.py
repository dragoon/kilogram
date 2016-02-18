"""
spark-submit --master yarn-client ./wikipedia/typograms/spark_organic_ngram_counts.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/organic_ngram_counts"
"""
import sys
from pyspark import SparkContext


sc = SparkContext(appName="WikipediaAnchors")

lines = sc.textFile(sys.argv[2])

# Split each line into words
def unpack_achors(line):
    ngram, uri_list = line.split('\t')
    total_count = sum(int(x.rsplit(",", 1)[1]) for x in uri_list.split(" "))
    return ngram.replace(" ", "_"), total_count


def filter_ambiguous(line):
    ngram, uri_list = line.split('\t')
    uri_set = set(x.split(',')[0].lower() for x in uri_list.split(' '))
    if len(uri_set) == 1:
        return True
    return False


anchor_counts = lines.filter(filter_ambiguous).map(unpack_achors)


dbp_data_file = sc.textFile(sys.argv[1])
# labels that have types
dbp_labels = dbp_data_file.filter(lambda line: bool(line.split('\t')[1])).map(lambda line: (line.split('\t')[0], 1)).distinct()
dbp_labels_lower = dbp_labels.map(lambda dbp_type: (dbp_type[0].lower(), dbp_type[0]))

anchors_join = dbp_labels.join(anchor_counts)
anchors_lower_join = dbp_labels_lower.join(anchor_counts)

anchors_lower_join = anchors_lower_join.map(lambda x: x[1])
anchors_join = anchors_join.map(lambda line: (line[0], line[1][1]))

type_anchors = anchors_join.fullOuterJoin(anchors_lower_join)
type_anchors.map(lambda x: x[0]+'\t'+ str(x[1][0] or 0)+','+str(x[1][1] or 0)).saveAsTextFile(sys.argv[3])
