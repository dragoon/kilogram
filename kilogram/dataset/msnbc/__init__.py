from __future__ import division
import os
import re
from kilogram.lang.tokenize import wiki_tokenize_func
from kilogram.lang import ne_dict
ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>(\'s)?')


def replace_types(context, ner):
    """
    :type context: unicode
    :type ner: NgramEntityResolver
    :return:
    """
    new_ngram = []
    for word in context:
        match = ENTITY_MATCH_RE.match(word)
        if match:
            uri_text = match.group(2).replace('_', ' ')
            uri = match.group(1)
            try:
                e_type = ner.get_type(ner.redirects_file.get(uri, uri), -1)
                new_ngram.append(e_type)
            except:
                new_ngram.extend(uri_text.split())
        else:
            new_ngram.append(word)
    return new_ngram


def get_context(start, text, match, ner):
    end = match.end() + start
    spaces = [i for i, c in enumerate(text) if c == ' ']
    try:
        prev_space = [i for i in spaces if i < start][-3]
    except IndexError:
        prev_space = 0
    try:
        next_space = [i for i in spaces if i >= end][2]
    except IndexError:
        next_space = len(text)

    prev_ngrams = replace_types(wiki_tokenize_func(text[prev_space+1:start]), ner)[-2:]
    while len(prev_ngrams) < 2:
        prev_ngrams.insert(0, 'NONE')

    next_ngrams = replace_types(wiki_tokenize_func(text[end:next_space]), ner)[:2]
    while len(next_ngrams) < 2:
        next_ngrams.append('NONE')

    return ' '.join(prev_ngrams) + ' NONE ' + ' '.join(next_ngrams)


def parse_data(data_dir, ner):
    data = {}
    for filename in os.listdir(data_dir):
        offset = 0
        data[filename.split('.')[0]] = {}
        text = ' '.join(open(data_dir+filename).readlines()).replace('\n', ' ')
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\'s\b', '', text)
        ner_dict = ne_dict(ENTITY_MATCH_RE.sub('\g<2>', text).replace('_', ' ').decode('utf-8'))
        for i, c in enumerate(text):
            if c == '<':
                # check end
                match = ENTITY_MATCH_RE.match(text[i:])
                if match:
                    uri_text = match.group(2).replace('_', ' ')
                    uri = match.group(1)
                    end_i = i + len(uri_text)
                    data[filename.split('.')[0]][(i - offset, end_i - offset)] = \
                        {'text': uri_text, 'uri': ner.redirects_file.get(uri, uri),
                         'ner': ner_dict.get(uri_text, None),
                         'context': get_context(i, text, match, ner)}
                    offset += (len(uri) + 3)
    return data
