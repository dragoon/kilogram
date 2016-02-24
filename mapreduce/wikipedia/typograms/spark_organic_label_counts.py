"""
spark-submit --master yarn-client ./wikipedia/typograms/spark_organic_ngram_counts.py "/user/roman/dbpedia_data.txt" "/user/roman/wiki_anchors" "/user/roman/organic_label_counts"
"""
import sys
from pyspark import SparkContext
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive


sc = SparkContext(appName="WikipediaAnchors")

lines = sc.textFile(sys.argv[2])

# Split each line into words
def unpack_achors(line):
    label, uri_list = line.split('\t')
    # tokenize for commas
    label = ' '.join(tokenize_possessive(default_tokenize_func(label)))
    # should be only one
    uri_count = uri_list.split(" ")[0]
    uri, count = uri_count.rsplit(',', 1)
    return label.lower(), (uri, label, count)


def filter_ambiguous(line):
    """
    Filter out anchors that have more than 1 uri
    """
    ngram, uri_list = line.split('\t')
    uri_set = set(x.rsplit(',', 1)[0].lower() for x in uri_list.split(' '))
    if len(uri_set) == 1:
        return True
    return False


anchor_counts = lines.filter(filter_ambiguous).map(unpack_achors)


dbp_data_file = sc.textFile(sys.argv[1])

anchor_counts_lower = anchor_counts.filter(lambda x: x[1][1].islower())
anchor_counts_normal = anchor_counts.filter(lambda x: not x[1][1].islower())

anchors_join = anchor_counts_lower.fullOuterJoin(anchor_counts_normal)

def map_join(elem):
    _, elem = elem
    lower_elem = elem[0]
    normal_elem = elem[1]
    lower_count = '0'
    normal_count = '0'
    if lower_elem:
        uri, label, lower_count = lower_elem
    if normal_elem:
        uri, label, normal_count = normal_elem
    return uri, (label, (normal_count, lower_count))

anchors_join = anchors_join.map(map_join)

# keep only uris that have types
dbp_uris_with_types = dbp_data_file.filter(lambda line: bool(line.split('\t')[1])).map(lambda line: (line.split('\t')[0], 1)).distinct()
anchor_counts_types = anchors_join.join(dbp_uris_with_types)

anchor_counts_types.map(lambda x: x[0]+'\t' + x[1][0][0] + '\t' + x[1][0][1][0]+','+x[1][0][1][1]).saveAsTextFile(sys.argv[3])
