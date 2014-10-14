# -*- coding: utf-8 -*-
from collections import defaultdict, Counter

import nltk
import re
from .ngram import EditNgram

PUNCT_SET = re.compile('[!(),.:;?/[\\]^`{|}]')


class EditCollection(object):
    """Collections of edit objects for Machine Learning and evaluation routines"""
    TOP_POS_TAGS = ['VB', 'NN', 'JJ', 'PR', 'RB', 'DT', 'OTHER']
    FEATURE_NAMES = [
        'avg_rank_1gram',        # 1
        'avg_rank_2gram',        # 2
        'avg_rank_3gram',        # 3
        'avg_pmi_1gram',         # 4
        'avg_pmi_2gram',         # 5
        'avg_pmi_3gram',         # 6
        'has_zero_ngram_2_or_3', # 7
        'zero_ngram_rank',       # 8
        'conf_matrix_score',      # 9
        'top_prep_count_1gram',  # 10
        'top_prep_count_2gram',  # 11
        'top_prep_count_3gram',  # 12
        'avg_rank_position_-1',      # 13
        'avg_rank_position_0',       # 14
        'avg_rank_position_1',       # 15
    ]
    collection = None

    def __init__(self, collection):
        """collection - array of Edit objects"""
        self.collection = sorted(collection, key=lambda x: x.is_error, reverse=True)
        self.labels = [int(edit.is_error) for edit in self.collection]

    def reverse_confusion_matrix(self):
        confusion_dict = defaultdict(Counter)
        for e in self.collection:
            confusion_dict[e.edit1][e.edit2] += 1
        return confusion_dict

    def _balance(self, class0_k, class1_k):
        """Balances collection with their respective coefficients for classes
        Collection should be sorted with 1 labels go before 0s.
        """
        import numpy as np
        from sklearn.utils import resample
        class1_count = len([1 for x in self.labels if x])
        class0_count = len(self.labels) - class1_count
        class1_col = self.collection[:class1_count]
        class0_col = self.collection[class1_count:]

        num_class0 = int(class0_count*class0_k)
        num_class1 = int(class1_count*class1_k)

        class0_col = resample(class0_col, replace=False, n_samples=num_class0)
        class1_col = resample(class1_col, replace=False, n_samples=num_class1)
        col = np.concatenate([class1_col, class0_col])
        labels = np.concatenate((np.ones(num_class1), np.zeros(num_class0)))
        return col, labels

    def balance_features(self, substitutions, class0_k=1., class1_k=1.):
        feature_names = self.FEATURE_NAMES[:]
        feature_names.extend(substitutions)
        feature_names.extend([x+'prev' for x in self.TOP_POS_TAGS])
        feature_names.extend([x+'next' for x in self.TOP_POS_TAGS])

        print('Balancing errors')
        data, _ = self._balance(class0_k, class1_k)
        data, labels = self.get_feature_array(data, substitutions)
        import numpy as np
        print('Converting to numpy arrays')
        data = np.array(data)
        labels = np.array(labels)
        print(data.shape)
        print(labels.shape)
        print('1st class', len([1 for x in labels if x]))
        print('2nd class', len([1 for x in labels if not x]))
        return data, labels, feature_names

    def get_feature_array(self, balanced_collection, substitutions):
        confusion_matrix = self.reverse_confusion_matrix()
        feature_collection = []
        feature_labels = []
        print('Generating features from raw data')
        for edit in balanced_collection:
            try:
                feature_vecs, labels = edit.get_single_feature(substitutions, self.TOP_POS_TAGS,
                                                               confusion_matrix)
            except AssertionError:
                continue
            feature_collection.extend(feature_vecs)
            feature_labels.extend(labels)
        return feature_collection, feature_labels

    def test_validation(self, substitutions, classifier, test_col):
        """
        :param classifier: any valid scikit-learn classifier
        """
        conf_matrix = self.reverse_confusion_matrix()

        def predict_substitution(edit, clf):
            top_suggestions = []
            try:
                features, labels = edit.get_single_feature(substitutions, self.TOP_POS_TAGS,
                                                           conf_matrix)
            except AssertionError:
                return None
            if not features:
                return None

            predictions = clf.predict_proba(features)
            for klasses, prep in zip(predictions, substitutions):
                klass0, klass1 = klasses
                if klass1 >= 0.5:
                    top_suggestions.append((klass1, prep))

            if top_suggestions:
                top_suggestions.sort(reverse=True, key=lambda x: x[0])
                prep = top_suggestions[0][1]
                return prep
            return None

        true_pos = 0
        false_pos = 0
        true_pos_err = 0
        false_pos_err = 0
        classifier.n_jobs = 1
        skips = 0
        skip_err = 0

        total_errors = len([1 for edit in test_col if edit.is_error])
        print('Total errors: %s' % total_errors)
        for edit in test_col:
            predicted_subst = predict_substitution(edit, classifier)
            if predicted_subst is None:
                skips += 1
                if edit.is_error:
                    skip_err += 1
                continue
            is_valid = False
            if edit.edit2 == predicted_subst:
                is_valid = True
            if is_valid:
                if edit.is_error:
                    true_pos_err += 1
                true_pos += 1
            else:
                if edit.is_error:
                    false_pos_err += 1
                false_pos += 1
        data = {'true': true_pos, 'false': false_pos, 'true_err': true_pos_err,
                'min_split': classifier.min_samples_split, 'depth': classifier.max_depth,
                'false_err': false_pos_err, 'skips': skips, 'skips_err': skip_err}
        try:
            precision = float(true_pos_err) / (true_pos_err + false_pos)
        except ZeroDivisionError:
            precision = 0.
        accuracy = float(true_pos) / (true_pos + false_pos)
        data['precision'] = precision
        data['accuracy'] = accuracy
        data['recall'] = float(true_pos_err)/total_errors
        if (data['recall'] + precision) > 0:
            data['f1'] = 2*precision*data['recall']/(data['recall'] + precision)
        else:
            data['f1'] = 0
        return data


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
        self.pos_tokens = None

    def init_pos_tags(self):
        self.pos_tokens = zip(*nltk.pos_tag(self.tokens))[1]

    def __unicode__(self):
        return self.edit1+u'→'+self.edit2 + u'\n' + u' '.join(self.context()).strip()

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def is_error(self):
        return self.edit1 != self.edit2

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
        pos_tag = bool(self.pos_tokens)
        for n_size in range(1, size):
            local_tokens = self.context(n_size, fill)
            local_indices = range(self.positions2[0]-n_size, self.positions2[1]+n_size)
            result_ngrams[n_size+1] = []
            local_tokens = Edit._reduce_punct(local_tokens, fill)

            for edit_pos, ngram, indices in zip(range(n_size, -1, -1),
                                                nltk.ngrams(local_tokens, n_size+1),
                                                nltk.ngrams(local_indices, n_size+1)):
                if fill in ngram:
                    result_ngrams[n_size+1].append(fill)
                else:
                    result_ngrams[n_size+1].append(EditNgram(ngram, edit_pos))
                    if pos_tag:
                        result_ngrams[n_size+1][-1].pos_tag = self.pos_tokens[indices[0]:indices[-1]+1]
        return result_ngrams

    def get_single_feature(self, SUBST_LIST, TOP_POS_TAGS, confusion_matrix, size=3):
        import pandas as pd
        if not self.pos_tokens:
            self.init_pos_tags()

        def get_pos_tag_features(bigrams):
            pos_tag_feature = []
            pos_tag_dict = dict([(bigram.edit_pos, [int(bigram.pos_tag[int(1 != bigram.edit_pos)] == x) for x in TOP_POS_TAGS])
                                 for bigram in bigrams if bigram])
            # append 1 or 0 whether POS tag is catch-all OTHER
            for key in pos_tag_dict.keys():
                if not any(pos_tag_dict[key]):
                    pos_tag_dict[key][-1] = 1
            for position in (0, 1):
                if position not in pos_tag_dict:
                    pos_tag_feature.extend([0 for _ in TOP_POS_TAGS])
                else:
                    pos_tag_feature.extend(pos_tag_dict[position])
            return pos_tag_feature

        context_ngrams = self.ngram_context(size)
        df_list_substs = []
        # RANK, PMI_SCORE
        DEFAULT_SCORE = (50, -10)
        # TODO: filter on ALLOWED_TYPES
        for ngram_type, ngrams in reversed(context_ngrams.items()):
            for ngram_pos, ngram in enumerate(ngrams):
                if ngram:
                    score_dict = dict((x[0][ngram_pos], (i, x[1])) for i, x in enumerate(ngram.association()))
                else:
                    score_dict = {}
                for subst in SUBST_LIST:
                    new_pos = 0
                    if ngram_pos == 0:
                        new_pos = -1
                    elif ngram_pos == (ngram_type - 1):
                        new_pos = 1
                    df_list_substs.append([subst, score_dict.get(subst, DEFAULT_SCORE)[1],
                                           score_dict.get(subst, DEFAULT_SCORE)[0], ngram_type,
                                           ngram_pos, new_pos])
        df_substs = pd.DataFrame(df_list_substs, columns=['substitution', 'score', 'rank', 'type', 'position', 'norm_position'])
        assert len(df_substs) > 0

        # TODO: takes longest zero prob, may be also add zero-prob length as a feature
        central_prob = df_substs[(df_substs.position != 0) &
                                 (df_substs.position != (df_substs.type-1))][:len(SUBST_LIST)].set_index('substitution')
        """type: DataFrame"""

        matrix = confusion_matrix[self.edit1]
        matrix_sum = sum(matrix.values())
        assert matrix_sum > 0

        feature_vectors = []
        labels = []

        # TODO: add indicator feature if rank/position is missing?
        type_group = df_substs.groupby(['substitution', 'type'])
        avg_by_position = df_substs.groupby(['substitution', 'norm_position']).mean()
        avg_by_type = type_group.mean()
        top_type_counts = type_group.apply(lambda x: x[x['rank'] == 0]['position'].count())

        for subst in SUBST_LIST:

            feature_vector = []
            # TODO: take only longest n-gram for position
            feature_vector.extend(list(avg_by_type.loc[subst]['rank'].values))
            feature_vector.extend(list(avg_by_type.loc[subst]['score'].values))
            # START: zero prob indicator feature -----
            feature_vector.append(int(central_prob.empty))
            if not central_prob.empty:
                feature_vector.append(central_prob.loc[subst]['rank'])
            else:
                feature_vector.append(50)
            # END zero prob
            feature_vector.append(matrix.get(subst, 0)/matrix_sum)

            # counts of a preposition on top of a ranking
            feature_vector.extend(list(top_type_counts.loc[subst].values))

            # average rank by normalized position
            feature_vector.extend(list(avg_by_position.loc[subst]['rank'].values))

            # substitutions themselves
            feature_vector.extend([int(x == subst) for x in SUBST_LIST])

            # POS TAG enumeration
            feature_vector.extend(get_pos_tag_features(context_ngrams[2]))

            labels.append(int(self.edit2 == subst))
            feature_vectors.append(feature_vector)
        return feature_vectors, labels
