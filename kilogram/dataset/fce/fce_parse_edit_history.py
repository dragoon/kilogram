#!/usr/bin/env python
"""
Extract grammatical error edits from FCE dataset
"""

import re
import argparse
import unicodecsv as csv
from zipfile import ZipFile
from lxml import etree
from lxml.etree import tostring

STRIP_NS_RE = re.compile(r'<NS type="{1,2}[A-Z]{1,3}"{1,2}/?>')

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-o', '--output', dest='out_file', action='store', required=True,
                    help='output TSV file')
parser.add_argument('files', nargs='+', help='input FCE ZIP file.')

args = parser.parse_args()


def _replace_tags(text, remove_tag, keep_tag):
    remove_tag_s = '<' + remove_tag + '>'
    remove_tag_e = '</' + remove_tag + '>'
    keep_tag_s = '<' + keep_tag + '>'
    keep_tag_e = '</' + keep_tag + '>'
    while text.find(remove_tag_e) != -1:
        c_end = text.find(remove_tag_e)
        c_start = text.rfind(remove_tag_s, 0, c_end)
        text = text[:c_start] + text[c_end+4:]
    text = text.replace(keep_tag_s, '').replace(keep_tag_e, '')
    return text


def filter_grammar_edits(in_files, out_file):
    with open(out_file, 'w') as out:
        csvwriter = csv.writer(out, delimiter='\t', encoding='utf-8')
        for in_file in in_files:
            with ZipFile(in_file) as zip_file:
                for xml_name in zip_file.namelist():
                    if xml_name.startswith('fce-released-dataset/dataset/')\
                            and xml_name.endswith('.xml'):
                        xml_contents = zip_file.read(xml_name)
                        try:
                            xml_contents = etree.fromstring(xml_contents)
                        except etree.XMLSyntaxError:
                            print 'XML exception in file %s' % xml_name
                            continue
                        for paragraph in xml_contents.findall('.//coded_answer/p'):
                            # We don't care about error types here
                            temp_text = tostring(paragraph).strip()
                            if '<unknown/>' in temp_text:
                                continue
                            temp_text = temp_text.replace('</NS>', '').replace('<p>', '').replace('</p>', '')
                            temp_text = STRIP_NS_RE.sub('', temp_text)

                            # get initial text
                            orig_text = _replace_tags(temp_text, 'c', 'i')
                            new_text = _replace_tags(temp_text, 'i', 'c')
                            if orig_text.isupper():
                                orig_text = orig_text.lower()
                                new_text = new_text.lower()
                            csvwriter.writerow([orig_text, new_text])
filter_grammar_edits(args.files, args.out_file)
