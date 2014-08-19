#!/usr/bin/env python
"""
Extract grammatical error edits from Wikipedia page histories
"""

import argparse
import unicodecsv as csv
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-o', '--output', dest='out_file', action='store', required=True,
                    help='output TSV file')
parser.add_argument('files', nargs='+', help='input 7z edit history files')

args = parser.parse_args()

from .libs.dewikify import Parser
wiki_parser = Parser()


def filter_grammar_edits(in_files, out_file):
    with open(out_file, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for wiki_7z_file in in_files:
            print 'Processing wiki %s ...' % wiki_7z_file
            p = subprocess.Popen('7zr e -so ' + wiki_7z_file, shell=True, stdout=subprocess.PIPE)
            # we have only one file in wiki
            cur_text = ''
            next_text = ''
            is_text = False
            is_next_text = False
            for line in p.stdout:
                line = line.lstrip()
                if line.startswith('<text xml:space="preserve">'):
                    is_text = True
                    if is_next_text:
                        next_text = line[27:]
                    else:
                        cur_text = line[27:]
                elif line.startswith('<comment>'):
                    comment = line[9:-11]
                    if 'grammar' in comment.lower():
                        is_next_text = True
                elif line.startswith('<'):
                    continue
                else:
                    if is_text:
                        if is_next_text:
                            next_text += line
                        else:
                            cur_text += line

                if line.rstrip().endswith('</text>'):
                    is_text = False
                    if is_next_text:
                        if line.startswith('<text xml:space="preserve">'):
                            next_text = line[27:-8]
                        else:
                            next_text = next_text[:-8]
                        is_next_text = False
                        cur_text = wiki_parser.parse_string(cur_text).replace('\n', ' ')
                        next_text_parsed = wiki_parser.parse_string(next_text).replace('\n', ' ')
                        csvwriter.writerow([cur_text.decode('utf-8'),
                                            next_text_parsed.decode('utf-8')])
                        # switch cur_text to next in case there are sequential grammar edits
                        cur_text = next_text
                    else:
                        if line.startswith('<text xml:space="preserve">'):
                            cur_text = line[27:-8]
                        else:
                            cur_text = cur_text[:-8]


filter_grammar_edits(args.files, args.out_file)
