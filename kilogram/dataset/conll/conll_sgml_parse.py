#!/usr/bin/env python
"""
Extract grammatical error edits from CoNLL dataset SGML files into kilogram format.
Not using XML or SGML parse since the file is not XML/SGML-compliant.
"""

import re
import argparse
import unicodecsv as csv

SPACE = re.compile(r'\s+')

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-o', '--output', dest='out_file', action='store', required=True,
                    help='output TSV file')
parser.add_argument('files', nargs='+', help='input SGML files')

args = parser.parse_args()

def extract_grammar_edits(in_files, out_file):
    """
    Extracts grammar edits from CoNLL SGML files
    :type out_file: str
    """
    with open(out_file, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for in_file in in_files:
            with open(in_file, 'r') as input_f:
                values = {}
                corrections = []
                for line in input_f:
                    # need to stack the corrections to replace from the end
                    if not line.startswith("<"):
                        # paragraph
                        paragraphs[-1]['orig'] += line.strip()
                        paragraphs[-1]['new'] += line.strip()
                    elif line.startswith('<TEXT>'):
                        paragraphs = []
                        corrections = []
                    elif line.startswith('<P>') or line.startswith('<TITLE>'):
                        paragraphs.append({'orig': '', 'new': ''})
                    elif line.startswith('</DOC>'):
                        nonoverlap_corrs = []
                        for values in corrections:
                            if values['type'] != 'Prep':
                                continue
                            if len(nonoverlap_corrs) > 0 and int(values['start_off']) >= nonoverlap_corrs[-1]['start_off'] and \
                                    int(values['end_off']) <= nonoverlap_corrs[-1]['end_off']:
                                continue
                            if len(nonoverlap_corrs) > 0 and int(values['start_off']) == nonoverlap_corrs[-1]['start_off'] and \
                                    int(values['end_off']) >= nonoverlap_corrs[-1]['end_off']:
                                # change offsets if overlapping replacement
                                nonoverlap_corrs[-1]['end_off'] = nonoverlap_corrs[-1]['start_off']

                            values['start_off'] = int(values['start_off'])
                            values['end_off'] = int(values['end_off'])
                            values['start_par'] = int(values['start_par'])
                            nonoverlap_corrs.append(values)
                        # make corrections
                        for values in reversed(nonoverlap_corrs):
                            new_par = paragraphs[values['start_par']]['new']
                            paragraphs[int(values['start_par'])]['new'] = ' '.join((new_par[:values['start_off']], values['correction'], new_par[values['end_off']:]))
                        # write paragraphs to output
                        for p in paragraphs:
                            # stip multiple spaces
                            p['new'] = SPACE.sub(' ', p['new'])
                            csvwriter.writerow([p['orig'], p['new']])
                    elif line.startswith('<MISTAKE'):
                        # update paragraph
                        values = dict([x.split('=') for x in line.strip()[9:-1].replace('"', '').split()])
                        if values['start_par'] != values['end_par']:
                            continue
                        corrections.append(values)
                    elif line.startswith('<CORRECTION>'):
                        values['correction'] = line.strip()[12:-13]
                    elif line.startswith("<TYPE>"):
                        values['type'] = line.strip()[6:-7]



extract_grammar_edits(args.files, args.out_file)