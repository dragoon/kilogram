__author__ = 'dragoon'
from kilogram import NgramService
import nltk
from ..lang.tokenize import default_tokenize_func



def _extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """
    table = "wiki_anchor_ngrams"
    column = "ngram:value"
    cand_entities = []
    noun_indexes = [i for i, word_token in enumerate(pos_tokens) if word_token[1].startswith('NN')]
    words = zip(*pos_tokens)[0]
    for noun_index in noun_indexes:
        n = 1
        while True:
            start_i = max(0, noun_index+1-n)
            end_i = min(len(words), noun_index+n)
            should_break = []
            for ngram in nltk.ngrams(words[start_i:end_i], n):
                ngram = ' '.join(ngram)
                res = NgramService.hbase_raw(table, ngram, column)
                if res:
                    cand_entities.append((start_i, end_i, res))
                    should_break.append(False)
                else:
                    should_break.append(True)
            if all(should_break):
                break
            if start_i == 0 and end_i == len(words)-noun_index:
                break
            n += 1
    return cand_entities


def link(sentence):
    pass
