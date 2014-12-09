#!/usr/bin/env python
"""
Extract grammatical error edits from CoNLL dataset SGML files into kilogram format.
Not using XML or SGML parse since the file is not XML/SGML-compliant.
"""

import argparse
import unicodecsv as csv


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-o', '--output', dest='out_file', action='store', required=True,
                    help='output TSV file')
parser.add_argument('files', nargs='+', help='input SGML files')

args = parser.parse_args()

def extract_grammar_edits(in_files, out_file):
    """
    Extracts grammar edits from CoNLL SGML files.
    :type out_file: str
    """
    with open(out_file, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for in_file in in_files:
            with open(in_file, 'r') as input_f:
                values = {}
                for line in input_f:
                    if not line.startswith("<"):
                        # paragraph
                        paragraphs[-1]['orig'] += line.strip()
                        paragraphs[-1]['new'] += line.strip()
                    elif line.startswith('<TEXT>'):
                        paragraphs = []
                    elif line.startswith('<P>'):
                        paragraphs.append({'orig': '', 'new': ''})
                    elif line.startswith('</DOC>'):
                        # write paragraphs to output
                        for p in paragraphs:
                            csvwriter.writerow([p['orig'], p['new']])
                    elif line.startswith('<MISTAKE'):
                        # update paragraph
                        values = dict([x.split('=') for x in line.strip()[9:-1].replace('"', '').split()])
                        if values['start_par'] != values['end_par']:
                            continue
                    elif line.startswith('<CORRECTION>'):
                        new_par = paragraphs[int(values['start_par'])]['new']
                        paragraphs[int(values['start_par'])]['new'] = new_par[:int(values['start_off'])] + line.strip()[12:-13] + new_par[int(values['end_off']):]


extract_grammar_edits(args.files, args.out_file)