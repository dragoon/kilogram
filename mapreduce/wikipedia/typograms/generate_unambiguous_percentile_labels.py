from __future__ import division
import argparse
import sys
from kilogram import ListPacker
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--percentile', default=0.9, type=float,
                    help='percentile to filter')
parser.add_argument('--min-count', default=1, type=int,
                    help='min count to filter')
args = parser.parse_args()


def filter_labels(line):
    label, uri_list = line.split('\t')
    # tokenize for commas
    label = ' '.join(tokenize_possessive(default_tokenize_func(label)))
    # should be only one
    uri_counts = [(uri, int(count)) for uri, count in ListPacker.unpack(uri_list)]
    total = sum(zip(*uri_counts)[1])

    for uri, count in uri_counts:
        if count/total > args.percentile and count > args.min_count:
            print(label + '\t' + uri + '\t' + str(count))
            break


for line in sys.stdin:
    filter_labels(line)
