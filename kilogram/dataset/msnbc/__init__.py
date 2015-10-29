from __future__ import division
import os
import re
from kilogram.lang import ne_dict, get_context

ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>(\'s)?')


def replace_types(context, ner):
    """
    :type context: list
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
                         'context': replace_types(get_context(i, text, match).split(), ner)}
                    offset += (len(uri) + 3)
    return data
