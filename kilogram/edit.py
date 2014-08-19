# -*- coding: utf-8 -*-
import nltk
import re
from .ngram import EditNgram

PUNCT_SET = re.compile('[!(),.:;?/[\\]^`{|}]')


class Edit(object):

    def __init__(self, edit1, edit2, text1, text2, positions1, positions2):
        self.edit1 = edit1
        self.edit2 = edit2
        self.text1 = text1
        self.text2 = text2
        self.positions1 = positions1
        self.positions2 = positions2
        # TODO: when edit is bigger than 1 word, need not to split it
        self.tokens = self.text2.split()
        # TODO: May be we want to POS tag here as well, cause we will need it to thow away useless 1grams

    def __unicode__(self):
        return self.edit1+u'→'+self.edit2 + u'\n' + u' '.join(self.context()).strip()

    def __str__(self):
        return unicode(self).encode('utf-8')

    @staticmethod
    def _reduce_punct(tokens, fill):
        # remove anything after/before punctuation
        center_index = len(tokens)//2
        punct_indexes = [i for i, ngram in enumerate(tokens)
                         if ngram != fill and PUNCT_SET.search(ngram)]
        left_indexes = [i for i in punct_indexes if i < center_index]
        left_indexes.append(0)
        right_indexes = [i for i in punct_indexes if i > center_index]
        right_indexes.append(len(tokens))
        left_index = max(left_indexes)
        right_index = min(right_indexes)
        return [fill]*left_index + tokens[left_index:right_index]\
            + [fill]*(len(tokens)-right_index)

    def context(self, size=3, fill=''):
        """Normal context"""
        left_index_orig = left_index = self.positions2[0]-size
        right_index_orig = right_index = self.positions2[1]+size
        if left_index < 0:
            left_index = 0
        if right_index > len(self.tokens):
            right_index = len(self.tokens)
        return [fill]*(-left_index_orig) + self.tokens[left_index:right_index]\
            + [fill]*(right_index_orig-len(self.tokens))

    def ngram_context(self, size=3, fill=''):
        """N-gram context"""
        result_ngrams = {}
        for n_size in range(1, size):
            local_tokens = self.context(n_size, fill)
            result_ngrams[n_size+1] = []
            local_tokens = Edit._reduce_punct(local_tokens, fill)

            for edit_pos, ngram in zip(range(n_size, -1, -1), nltk.ngrams(local_tokens, n_size+1)):
                if fill in ngram:
                    result_ngrams[n_size+1].append(fill)
                else:
                    result_ngrams[n_size+1].append(EditNgram(ngram, edit_pos))
        return result_ngrams
