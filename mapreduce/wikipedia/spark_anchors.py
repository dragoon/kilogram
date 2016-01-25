"""
spark-submit --executor-memory 5g --master yarn-client ./wikipedia/spark_anchors.py "/user/ded/link_mention" "/user/roman/wiki_anchors" "/user/roman/wiki_urls"
pig -p table=wiki_anchors -p path=/user/roman/wiki_anchors ./hbase_upload_array.pig
pig -p table=wiki_urls -p path=/user/roman/wiki_urls ./hbase_upload_array.pig
"""
from pyspark import SparkContext
import urllib
import argparse
from kilogram.lang.unicode import strip_unicode

sc = SparkContext(appName="WikipediaAnchors")

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--lowercase', dest='is_lowercase', action='store_true', required=False,
                    default=False, help='whether to lowercase the mentions or not')
parser.add_argument('link_mention_dir',
                    help='path to the link_mention directory on HDFS')
parser.add_argument('wiki_anchors_out_dir',
                    help='output path of the wiki anchors directory on HDFS')
parser.add_argument('wiki_urls_out_dir',
                    help='output path of the wiki urls directory on HDFS')

args = parser.parse_args()

lines = sc.textFile(args.link_mention_dir)

# Split each line into words
def unpack_anchors(line):
    line = strip_unicode(line)
    uri, mention = line.split('\t')
    uri = uri.replace(' ', '_')
    mention = mention.replace('_', ' ')
    if args.lowercase:
        mention = mention.lower()
    return uri[0].upper()+uri[1:], mention

anchor_counts = lines.map(unpack_anchors)


dbp_redirects_file = sc.textFile("/user/roman/redirects_transitive_en.nt.bz2")
def map_redirects(line):
    try:
        uri, _, canon_uri, _ = line.split()
    except:
        return None, None
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    if '/' in uri:
        return None, None
    canon_uri = canon_uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    # return only redirect, we'll do left join later
    return uri, urllib.unquote(canon_uri.encode('ascii')).decode('utf-8')

dbp_urls = dbp_redirects_file.map(map_redirects).distinct()
dbp_anchor_join = anchor_counts.leftOuterJoin(dbp_urls)

anchor_counts_join = dbp_anchor_join.map(lambda x: x[1] if x[1][1] else (x[1][0], x[0]))
uri_counts_join = dbp_anchor_join.map(lambda x: (x[1][1], x[1][0]) if x[1][1] else (x[0], x[1][0]))


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
uri_counts_agg = uri_counts_join.aggregateByKey({}, seqfunc, combfunc)

def printer(value):
    return value[0] + '\t' + ' '.join([x.replace(' ', '_')+","+str(y) for x, y in value[1].items()])

anchor_counts_agg.map(printer).saveAsTextFile(args.wiki_anchors_out_dir)
uri_counts_agg.map(printer).saveAsTextFile(args.wiki_anchors_out_dir)
