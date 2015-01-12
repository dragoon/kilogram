import difflib
import re
import socket

import unicodecsv as csv

from ..edit import Edit
from pyutils import print_progress
from kilogram.lang import strip_determiners
from .tokenize import default_tokenize_func

MULTIPLE_PUNCT_REGEX = re.compile(r'([.!?-]){2,}')
MULTIPLE_SPACE_REGEX = re.compile(r'([ ]){2,}')
GARBAGE_REGEX = re.compile(r'[^\w\s]')
ADJ_REGEX = re.compile(r'([^\s]+_JJ\w?\s?)+ (?=[^\s]+_NN.?)')


def _get_line_num(edit_file):
    line_n = 0
    with open(edit_file) as input:
        line_n = sum(1 for _ in input)
    return line_n


def _prefilter_line(row):
    edits = []
    for edit in row:
        edit = edit.strip()
        if '\n' in edit:
            print 'LOL'
            return
        # replace special formatting
        edit = edit.replace('*', '')
        edit = MULTIPLE_PUNCT_REGEX.sub('\g<1>', edit)
        edit = MULTIPLE_SPACE_REGEX.sub('\g<1>', edit)
        if not edit or edit == "null":
            edit = None
        edits.append(edit)
    return edits


def _is_garbage(ngram1, ngram2):
    """Filter useless edits"""
    if not ngram1 and not ngram2:
        return True
    elif ngram1.lower() == ngram2.lower():
        return True
    ngram1 = GARBAGE_REGEX.search(ngram1)
    ngram2 = GARBAGE_REGEX.search(ngram2)
    if ngram1 or ngram2:
        return True
    return False


def _init_pos_tags(tokens):
    from .. import ST_HOSTNAME, ST_PORT
    def _pos_tag_socket(hostname, port, content):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((hostname, port))
        s.sendall(content.encode('utf-8'))
        s.shutdown(socket.SHUT_WR)
        data = ""
        while 1:
            l_data = s.recv(8192)
            if l_data == "":
                break
            data += l_data
        s.close()
        return data

    def compress_pos(pos_tag):
        if pos_tag.startswith('VB'):
            pos_tag = 'VB'
        elif pos_tag == 'NNS':
            pos_tag = 'NN'
        return pos_tag
    pos_tokens = _pos_tag_socket(ST_HOSTNAME, ST_PORT, ' '.join(tokens)).strip()
    return [compress_pos(x.split('_')[1]) for x in pos_tokens.split()]


def extract_edits(edit_file, substitutions=None, tokenize_func=default_tokenize_func):
    """
    Extracts contexts for all n-grams that were changed between two text versions.
    Uses most sequence matcher from difflib, which takes most longest subsequence.

    If ``substitutions'' argument is supplied, extract all n-gram matching substitutions,
    even if they were not changed.

    :returns: list of Edit objects
    """
    edit_n = 0
    edits = []
    line_n = _get_line_num(edit_file)

    with open(edit_file, 'r') as input:
        csvreader = csv.reader(input, delimiter='\t', encoding='utf-8')
        csvreader.__class__.__len__ = lambda x: line_n
        for row in print_progress(csvreader):
            row = _prefilter_line(row)
            edit1, edit2 = row
            if edit1 is None or edit2 is None:
                continue
            # tokenize to words, since we want word diff
            edit1 = tokenize_func(edit1)
            edit2 = tokenize_func(edit2)
            context1 = strip_determiners(' '.join(edit1))
            context2 = strip_determiners(' '.join(edit2))
            edit1 = context1.split()
            edit2 = context2.split()
            pos_tokens = _init_pos_tags(edit2)
            for seq in difflib.SequenceMatcher(None, edit1, edit2).get_grouped_opcodes(0):
                for tag, i1, i2, j1, j2 in seq:
                    if tag == 'equal':
                        continue
                    ngram1, ngram2 = ' '.join(edit1[i1:i2]), ' '.join(edit2[j1:j2])
                    #if _is_garbage(ngram1, ngram2):
                    #    continue
                    # TODO: works only for unigram substitutions
                    # extract merged edits into unigrams that match substitutions
                    if substitutions:
                        index1 = [(ix, i) for ix, i in enumerate(range(i1, i2)) if edit1[i] in substitutions]
                        index2 = [(ix, i) for ix, i in enumerate(range(j1, j2)) if edit2[i] in substitutions]
                        if len(index1) != 1 or len(index2) != 1 or index1[0][0] != index2[0][0]:
                            continue
                        ngram1, ngram2 = edit1[index1[0][1]], edit2[index2[0][1]]
                        i1, i2 = index1[0][1], index1[0][1]+1
                        j1, j2 = index2[0][1], index2[0][1]+1
                    edits.append(Edit(ngram1, ngram2, context1, context2, (i1, i2), (j1, j2), pos_tokens))
                    edit_n += 1

            # Add all other substitution if supplied
            # TODO: works only for unigrams
            if substitutions:
                for i, unigram in enumerate(edit2):
                    if unigram in substitutions:
                        edits.append(Edit(unigram, unigram, context1, context2, (i, i+1), (i, i+1), pos_tokens))
                        edit_n += 1

        del csvreader.__class__.__len__
    print 'Total edits extracted:', edit_n
    return edits


def extract_filtered(edit_file, filter_func, tokenize_func=default_tokenize_func):
    """
    Extracts contexts for all words from edit_file that satisfy conditions in ``filter_func``.
    Only second text version is used.
    :returns: list of Edit objects
    """
    edit_n = 0
    edits = []
    line_n = _get_line_num(edit_file)
    with open(edit_file, 'r') as input:
        csvreader = csv.reader(input, delimiter='\t', encoding='utf-8')
        csvreader.__class__.__len__ = lambda x: line_n
        for row in print_progress(csvreader):
            row = _prefilter_line(row)
            _, edit2 = row
            if edit2 is None:
                continue
            # tokenize to words, since we want word diff
            edit2 = tokenize_func(edit2)
            context2 = ' '.join(edit2)
            for i1, word in enumerate(edit2):
                if filter_func(word):
                    edits.append(Edit(word, word, context2, context2, (i1, i1+1), (i1, i1+1)))
                    edit_n += 1
        del csvreader.__class__.__len__
    print 'Total edits extracted:', edit_n
    return edits
