from __future__ import division
from collections import defaultdict
import shelve

from scipy.stats import entropy, kurtosis

from .. import NgramService, SUBSTITUTION_TOKEN, ListPacker


def parse_counts(filename):
    counts = {}
    with open(filename) as f:
        for l in f:
            line = l.strip().split('\t')
            found = line[3][1:-1].split(',')
            found_nums = [int(x[1:]) for x in found[0:][::2]]
            found_types = [x[:-1] for x in found[1:][::2]]
            counts[line[1] + " " + line[2]] = dict(zip(found_types, found_nums))
    return counts


class NgramTypePredictor(object):
    type_hierarchy = None
    hbase_table = None
    type_priors = None

    def __init__(self, hbase_table, type_hierarchy=None):
        self.type_hierarchy = type_hierarchy
        self.hbase_table = hbase_table
        self.type_priors = {}
        total = sum(NgramService.substitution_counts.values())
        for entity_type, count in NgramService.substitution_counts.items():
            self.type_priors[entity_type] = count/total

    def _get_ngram_probs(self, context):
        # pre, post, mid bigrams
        context[2] = SUBSTITUTION_TOKEN
        trigrams = [context[:3], context[-3:], context[1:4]]
        bigrams = [context[1:3], None, context[2:4]]

        types = []
        for bigram, trigram in zip(bigrams, trigrams):
            type_values = NgramService.hbase_raw(self.hbase_table, " ".join(trigram), "ngram:value")
            if not type_values and bigram:
                type_values = NgramService.hbase_raw(self.hbase_table, " ".join(bigram), "ngram:value")
            if type_values:
                types.append(ListPacker.unpack(type_values))
        totals = [sum(int(x) for x in zip(*type_values)[1]) for type_values in types]
        probs = [[(entity_type, int(count)/totals[i]) for entity_type, count in type_values]
                        for i, type_values in enumerate(types)]
        return probs

    def predict_types(self, context):
        """Context should always be a 5-element list"""
        ngram_probs = self._get_ngram_probs(context)
        type_probs = defaultdict(lambda: 0)
        for probs in ngram_probs:
            for entity_type, prob in probs:
                type_probs[entity_type] += prob/len(ngram_probs)
        try:
            min_prob = min(type_probs.values())
        except ValueError:
            # empty sequence
            min_prob = 1
        for entity_type, prior in self.type_priors.items():
            if entity_type not in type_probs:
                type_probs[entity_type] = prior*min_prob

        result_probs = sorted(type_probs.items(), key=lambda x: x[1], reverse=True)
        return result_probs

    def predict_types_full(self, context):
        """Context should always be a 5-element list
        """
        ngram_probs = self._get_ngram_probs(context)
        return self._hierachical_sum_output(ngram_probs)

    def _hierachical_sum_output(self, bigram_probs):
        correct_types = []
        while True:
            if not correct_types:
                parents = [(None, 0)]
            else:
                parents = correct_types[-1]
            correct_types_local = []
            for parent, _score in parents:
                parent_probs = self._sum_probabilities(bigram_probs, parent)

                if len(parent_probs) == 0:
                    break
                type_probs = defaultdict(lambda: 0)
                for probs in parent_probs:
                    for entity_type, prob in probs:
                        type_probs[entity_type] += prob
                correct_types_local.extend(sorted(type_probs.items(), key=lambda x: x[1], reverse=True))
            if len(correct_types_local) == 0:
                break
            correct_types.append(correct_types_local)
        return correct_types

    def _sum_probabilities(self, probabilities_list, parent):
        new_prob_list = []
        for probabilities in probabilities_list:
            prob_dict = defaultdict(lambda: 0)
            for entity_type, prob in probabilities:
                entity_type = self.type_hierarchy.get_parent(entity_type, parent)
                prob_dict[entity_type] += prob
            try:
                none_prob = prob_dict[None]/(len(prob_dict) - 1)
                del prob_dict[None]
                for key in prob_dict.keys():
                    prob_dict[key] += none_prob

                new_prob_list.append(prob_dict.items())
            except:
                continue
        return new_prob_list
