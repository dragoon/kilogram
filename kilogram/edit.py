# -*- coding: utf-8 -*-
from __future__ import division
from collections import defaultdict, Counter
import functools
import multiprocessing

from datetime import datetime

import nltk
from .lang import number_replace, pos_tag
from .ngram import EditNgram


def get_single_feature_classify(edit):
    try:
        return edit.get_single_feature(EditCollection.SUBSTITUTIONS)
    except AssertionError:
        print 'IGNORED EDIT:', edit
        print
        return None


def get_single_feature_detect(edit):
    try:
        return edit.get_single_feature([edit.edit1])
    except AssertionError:
        print 'IGNORED EDIT:', edit
        print
        return None


# ngram importance regressor
NGRAM_REGRESSOR = None


class EditCollection(object):
    """Collections of edit objects for Machine Learning and evaluation routines"""
    TOP_POS_TAGS = ['VB', 'NN', 'JJ', 'RB', 'DT', 'OTHER']
    FEATURE_NAMES = [
        'avg_rank_2gram',        # 1
        'has_avg_2gram',         # 2
        'avg_rank_3gram',        # 3
        'has_avg_3gram',         # 4
        'avg_pmi_3gram',         # 5
        'len_zero_ngram',        # 7
        'zero_ngram_rank',       # 8
        'conf_matrix_score',     # 9
        'top_prep_count_2gram',  # 10
        'top_prep_count_3gram',  # 11
        'avg_rank_position_-1',  # 13
        'avg_rank_position_0',   # 14
        'avg_rank_position_1',   # 15
    ]
    CONFUSION_MATRIX = None
    SUBSTITUTIONS = None

    def __init__(self, collection, substitutions, feature_func=get_single_feature_classify):
        """collection - array of Edit objects"""
        self.collection = sorted(collection, key=lambda x: x.is_error, reverse=True)
        self.labels = [int(edit.is_error) for edit in self.collection]
        self.__class__.CONFUSION_MATRIX = self._reverse_confusion_matrix()
        self.test_errors = None
        self.test_skip_errors = None
        self.test_false_errors = None
        self.test_correct_positions = None
        self.__class__.SUBSTITUTIONS = substitutions
        self.feature_func = feature_func

    def _reverse_confusion_matrix(self):
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

        class0_col = resample(class0_col, replace=False, n_samples=num_class0, random_state=1)
        class1_col = resample(class1_col, replace=False, n_samples=num_class1, random_state=1)
        col = np.concatenate([class1_col, class0_col])
        labels = np.concatenate((np.ones(num_class1), np.zeros(num_class0)))
        return col, labels

    def balance_features(self, class0_k=1., class1_k=1.):
        feature_names = self.FEATURE_NAMES[:]
        feature_names.extend(self.SUBSTITUTIONS)
        feature_names.extend([x+'prev' for x in self.TOP_POS_TAGS])
        feature_names.extend([x+'next' for x in self.TOP_POS_TAGS])

        print('Balancing errors')
        data, _ = self._balance(class0_k, class1_k)
        data, labels = self.get_feature_array(data)

        import numpy as np
        print('Converting to numpy arrays')
        data = np.array(data)
        labels = np.array(labels)
        print(data.shape)
        print(labels.shape)
        print('1st class', len([1 for x in labels if x]))
        print('2nd class', len([1 for x in labels if not x]))
        return data, labels, feature_names

    def get_feature_array(self, balanced_collection):
        features = []
        labels = []
        print('Generating features from raw data')

        # multiprocessing association measures population
        pool = multiprocessing.Pool(12)
        print 'Started data loading: {0:%H:%M:%S}'.format(datetime.now())

        get_single_feature1 = functools.partial(self.feature_func)
        collection = pool.map(get_single_feature1, balanced_collection)
        print 'Finish data loading: {0:%H:%M:%S}'.format(datetime.now())

        for features_labels in collection:
            # avoid assertion errors
            if features_labels is not None:
                local_features, local_labels = features_labels
                features.extend(local_features)
                labels.extend(local_labels)
        return features, labels

    def test_validation(self, classifier, test_col):
        """
        :param classifier: any valid scikit-learn classifier
        """
        self.test_errors = []
        self.test_skip_errors = []
        self.test_false_errors = []
        self.test_correct_positions = []

        pool = multiprocessing.Pool(12)
        print 'Started data loading: {0:%H:%M:%S}'.format(datetime.now())

        get_single_feature1 = functools.partial(self.feature_func)
        test_collection = pool.map(get_single_feature1, test_col)
        print 'Finish data loading: {0:%H:%M:%S}'.format(datetime.now())

        def predict_substitution(features, clf, correct_edit):
            if not features:
                return None

            predictions = clf.predict_proba(features)
            predictions = sorted([(prep, classes[1]) for classes, prep in zip(predictions, self.SUBSTITUTIONS)],
                                 key=lambda x: x[1], reverse=True)
            self.test_correct_positions.append([i for i, x in enumerate(predictions) if x[0] == correct_edit][0])
            top_suggestions = [prep for prep, confidence in predictions if confidence >= 0.5]

            return top_suggestions or None

        true_pos = 0
        false_pos = 0
        true_pos_err = 0
        false_pos_err = 0
        classifier.n_jobs = 1
        skips = 0
        skip_err = 0

        total_errors = len([1 for edit in test_col if edit.is_error])
        print('Total errors: %s' % total_errors)
        for edit, labels_features in zip(test_col, test_collection):
            # avoid assertion errors
            if labels_features is None:
                continue
            features = labels_features[0]
            predicted_substs = predict_substitution(features, classifier, edit.edit2)
            if predicted_substs is None or edit.edit1 in predicted_substs:  # -- improves F1 by ~ 1%: evaluate more
                skips += 1
                if edit.is_error:
                    self.test_skip_errors.append((edit, predicted_substs))
                    skip_err += 1
                continue
            is_valid = False
            if edit.edit2 == predicted_substs[0]:
                is_valid = True
            if is_valid:
                if edit.is_error:
                    true_pos_err += 1
                true_pos += 1
            else:
                if edit.is_error:
                    self.test_errors.append((edit, predicted_substs))
                    false_pos_err += 1
                else:
                    self.test_false_errors.append((edit, predicted_substs))
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
    # increases F1 by ~6-7%
    IGNORE_TAGS = {'DT', 'PR', 'TO', 'CD', 'WD', 'WP'}  # CD doesn't improve - why?

    def __init__(self, tokens1, tokens2, positions1, positions2):

        self.edit1 = [x.lower() for x in tokens1[slice(*positions1)]]
        self.edit2 = [x.lower() for x in tokens2[slice(*positions2)]]
        self.positions1 = positions1
        self.positions2 = positions2
        self.left_tokens = tokens2[:positions2[0]]
        self.right_tokens = tokens2[positions2[1]:]

        pos_tokens = pos_tag(' '.join(tokens2))
        self.left_pos_tokens = pos_tokens[:positions2[0]]
        self.right_pos_tokens = pos_tokens[positions2[1]:]
        self.edit_pos_tokens = pos_tokens[slice(*positions2)]
        self._ngram_context = {}

    def __unicode__(self):
        return self.edit1+u'→'+self.edit2 + u'\n' + u' '.join(self.context()).strip()

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def is_error(self):
        return self.edit1 != self.edit2

    @staticmethod
    def _lowercase_token(token):
        if token == '.':
            return ','
        return number_replace(token.lower())

    def context(self, size=3, fill='', pos_tagged=False):
        """Normal context"""
        def context_tokens(left, center, right):
            return [fill] * (size - len(left)) + \
                left[-size:] + center + right[:size] + \
                [fill] * (size - len(right))
        ct = context_tokens(self.left_tokens, self.edit2, self.right_tokens)
        if pos_tagged:
            pos_tokens = context_tokens(self.left_pos_tokens, self.edit_pos_tokens, self.right_pos_tokens)
            ct = zip(ct, pos_tokens)
        return ct

    def ngram_context(self, size=3, fill=''):
        """N-gram context"""
        if size in self._ngram_context:
            return self._ngram_context[size]
        result_ngrams = {}
        for n_size in range(1, size):
            context_tokens = self.context(n_size, fill, pos_tagged=True)
            result_ngrams[n_size+1] = []

            for edit_pos, ngram in zip(range(n_size, -1, -1), nltk.ngrams(context_tokens, n_size+1)):
                ngram, ngram_pos_tag = zip(*ngram)
                if fill in ngram:
                    result_ngrams[n_size+1].append(fill)
                else:
                    result_ngrams[n_size+1].append(EditNgram(ngram, edit_pos))
                    result_ngrams[n_size+1][-1].pos_tag = ngram_pos_tag
        self._ngram_context[size] = result_ngrams
        return result_ngrams

    def get_single_feature(self, SUBS_LIST, size=3):
        import pandas as pd

        def is_useful(pos_seq):
            """Manually marked useless pos sequences, such a DT, PRP$, etc."""
            result = True
            pos_set = [x[:2] for x in pos_seq]
            if len(pos_seq) == 2 and Edit.IGNORE_TAGS.intersection(pos_set):
                result = False
            return result

        def get_pos_tag_features(bigrams):
            pos_tag_feature = []
            pos_tag_dict = dict([(bigram.edit_pos,
                                  [int(bigram.pos_tag[int(1 != bigram.edit_pos)] == x) for x in EditCollection.TOP_POS_TAGS])
                                 for bigram in bigrams if bigram])
            # append 1 or 0 whether POS tag is catch-all OTHER
            for key in pos_tag_dict.keys():
                if not any(pos_tag_dict[key]):
                    pos_tag_dict[key][-1] = 1
            for position in (0, 1):
                if position not in pos_tag_dict:
                    pos_tag_feature.extend([0 for _ in EditCollection.TOP_POS_TAGS])
                else:
                    pos_tag_feature.extend(pos_tag_dict[position])
            return pos_tag_feature

        context_ngrams = self.ngram_context(size)
        df_list_substs = []
        # RANK, PMI_SCORE
        DEFAULT_SCORE = (50, -10)
        # TODO: filter on ALLOWED_TYPES
        added_normal_positions = set()
        for ngram_type, ngrams in reversed(context_ngrams.items()):
            for ngram_pos, ngram in enumerate(ngrams):
                if not ngram:
                    continue
                if not is_useful(ngram.pos_tag):
                    continue

                norm_pos = ngram.normal_position
                if norm_pos in added_normal_positions:
                    continue
                else:
                    score_dict = ngram.association()
                    if not score_dict:
                        continue
                    #added_normal_positions.add(norm_pos)

                ngram_weight = 0
                if NGRAM_REGRESSOR:
                    ngram_weight = NGRAM_REGRESSOR.predict(ngram.get_single_feature(self.edit2)[0])[0]
                for subst in SUBS_LIST:
                    df_list_substs.append([subst, score_dict.get(subst, DEFAULT_SCORE)[1],
                                           score_dict.get(subst, DEFAULT_SCORE)[0],
                                           ngram_type, norm_pos, 1/(ngram_weight+1)])
        assert len(df_list_substs) > 0
        df_substs = pd.DataFrame(df_list_substs, columns=['substitution', 'score', 'rank', 'type', 'norm_position', 'weight'])

        central_prob = df_substs[(df_substs.norm_position == 0)][:len(SUBS_LIST)].set_index('substitution')
        """type: DataFrame"""

        matrix = EditCollection.CONFUSION_MATRIX[self.edit1]
        matrix_sum = sum(matrix.values())
        assert matrix_sum > 0

        feature_vectors = []
        labels = []

        # TODO: add indicator feature if rank/position is missing?
        type_group = df_substs.groupby(['substitution', 'type'])
        avg_by_position = df_substs.groupby(['substitution', 'norm_position']).mean()
        avg_by_type = type_group.mean()
        #weighted_ranks_types = type_group.apply(lambda subf: sum(subf['weight']*subf['rank'])/sum(subf['weight']))
        #weighted_scores_types = type_group.apply(lambda subf: sum(subf['weight']*subf['score'])/sum(subf['score']))
        #weighted_ranks_positions = df_substs.groupby(['substitution', 'norm_position']).apply(lambda subf: sum(subf['weight']*subf['rank'])/sum(subf['weight']))
        top_type_counts = type_group.apply(lambda x: x[x['rank'] == 0]['rank'].count())

        for subst in SUBS_LIST:

            feature_vector = []
            for ngram_size in range(2, 4):
                feature_vector.append(avg_by_type.loc[subst]['rank'].get(ngram_size, 50))
                feature_vector.append(int(feature_vector[-1] != 50))
            feature_vector.append(avg_by_type.loc[subst]['score'].get(3, -10))

            # START: zero prob indicator feature -----
            central_prob_len = 0
            if len(central_prob) > 0:
                central_prob_len = central_prob['type'].values[0]
            feature_vector.append(central_prob_len)
            feature_vector.append(central_prob['rank'].get(subst, 50))
            # END zero prob

            # reverse confusion matrix
            feature_vector.append(matrix.get(subst, 0)/matrix_sum)
            # counts of a preposition on top of a ranking
            for ngram_size in range(2, 4):
                feature_vector.append(top_type_counts.loc[subst].get(ngram_size, 0))

            # average rank by normalized position
            for position in (-1, 0, 1):
                feature_vector.append(avg_by_position.loc[subst]['rank'].get(position, 50))

            # substitutions themselves
            feature_vector.extend([int(x == subst) for x in EditCollection.SUBSTITUTIONS])

            # POS TAG enumeration
            feature_vector.extend(get_pos_tag_features(context_ngrams[2]))

            labels.append(int(self.edit2 == subst))
            feature_vectors.append(feature_vector)
        return feature_vectors, labels
