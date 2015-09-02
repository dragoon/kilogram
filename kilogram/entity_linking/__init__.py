__author__ = 'dragoon'
from kilogram import NgramService
from ..lang.tokenize import default_tokenize_func


def _extract_candidates(sentence):
    """
    :type sentence: unicode
    :return:
    """
    table = "wiki_anchor_ngrams"
    column = "ngram:value"
    i = 0
    j = 1
    tokens = default_tokenize_func(sentence)
    prev_res = None
    cand_entities = []
    while j < len(tokens):
        ngram = ' '.join(tokens[i:j])
        res = NgramService.hbase_raw(table, ngram, column)
        if res:
            prev_res = res
            # try further
            j += 1
        elif prev_res:
            cand_entities.append((i, j-1, prev_res))
            prev_res = None
            i += 1
        else:
            i += 1
            j += 1
    return cand_entities


def link(sentence):
    pass
