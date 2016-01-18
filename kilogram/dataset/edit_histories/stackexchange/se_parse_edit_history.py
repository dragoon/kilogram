#!/usr/bin/env python
"""
Extract grammatical error edits from StackExchange post histories
"""

import argparse
import re
import os.path
import unicodecsv as csv
import subprocess
import string

from lxml.etree import iterparse


SPACE = re.compile(r'\s+')
LINEBREAK = re.compile(r'.?(\r\n)+')
_TEMP_FILE = '/tmp/se_out.tsv'

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-o', '--output', dest='out_file', action='store', required=True,
                    help='output TSV file')
parser.add_argument('files', nargs='+', help='input XML history files')

args = parser.parse_args()


def _sort_posts_by_id(in_file):
    """
    Processes StackExchange XML edit history file and orders edits by post id.
    By default they are sorted by edit date.
    """
    # get an iterable and turn it into an iterator
    context = iter(iterparse(in_file, events=("start", "end")))

    # get the root element
    event, root = next(context)
    assert event == "start"

    print "Processing XML %s ..." % in_file
    with open(_TEMP_FILE, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for event, elem in context:
            if event == "end" and elem.tag == "row":
                hist_id = int(elem.get('PostHistoryTypeId'))
                row_id = int(elem.get('Id'))
                if hist_id in (1, 2, 4, 5):
                    try:
                        post_id = elem.get('PostId')
                        if hist_id in (1, 4):
                            post_id += '-title'
                        text = elem.get('Text')
                        # strip multiple spaces and replace line breaks with dots
                        text = LINEBREAK.sub('.', text)
                        text = SPACE.sub(' ', text).strip()
                        text = filter(lambda x: x in string.printable, text)
                        comment = elem.get('Comment', '')
                        # strip multiple spaces
                        comment = SPACE.sub(' ', comment)
                        csvwriter.writerow([post_id, text, comment.lower()])
                    except Exception, e:
                        print 'Exception occurred:', e
                        print 'Row ID:', row_id
                root.clear()

    # sort output file with linux sort
    print "Sorting..."
    subprocess.call("sort -o %s -k1,1 -s %s" % (_TEMP_FILE, _TEMP_FILE), shell=True)


def filter_grammar_edits(in_files, out_file):
    """
    Extracts grammar edits from the file obtained by convert_to_sorted_csv
    :type out_file: str
    """
    with open(out_file, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for in_file in in_files:
            _sort_posts_by_id(in_file)
            cur_post_id = None
            cur_text = None

            print "Filtering edits..."
            if not os.path.exists(_TEMP_FILE):
                raise Exception("Need to call sort_posts_by_id first")

            with open(_TEMP_FILE, 'r') as input:
                csvreader = csv.reader(input, delimiter='\t', encoding='utf-8')
                for row in csvreader:
                    post_id, text, comment = row
                    if 'grammar' in comment or 'spelling' in comment:
                        if post_id != cur_post_id:
                            print 'not in order', post_id, cur_post_id
                            continue
                        csvwriter.writerow([cur_text, text])
                    cur_text = text
                    cur_post_id = post_id


filter_grammar_edits(args.files, args.out_file)