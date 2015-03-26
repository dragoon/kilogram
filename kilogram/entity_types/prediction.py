from __future__ import division
from collections import defaultdict

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


class TypePredictor(object):
    type_hierarchy = None
    hbase_table = None

    def __init__(self, hbase_table, type_hierarchy=None):
        self.type_hierarchy = type_hierarchy
        self.hbase_table = hbase_table

    def _get_ngram_probs(self, context):
        # pre, post, mid bigrams
        context[2] = SUBSTITUTION_TOKEN
        bigrams = [context[:3], context[-3:], context[1:4]]
        types = []
        for bigram in bigrams:
            type_values = NgramService.hbase_raw(self.hbase_table, " ".join(bigram), "ngram:value")
            if type_values:
                types.append(ListPacker.unpack(type_values))
        totals = [sum(int(x) for x in zip(*type_values)[1]) for type_values in types]
        bigram_probs = [[(entity_type, int(count)/totals[i]) for entity_type, count in type_values]
                        for i, type_values in enumerate(types)]
        return bigram_probs


    def predict_types(self, context):
        """Context should always be a 5-element list"""
        bigram_probs = self._get_ngram_probs(context)
        #bigram_probs = [probs for probs in bigram_probs if entropy(zip(*probs)[1]) < 3]
        type_probs = defaultdict(lambda: 0)
        for probs in bigram_probs:
            for entity_type, prob in probs:
                type_probs[entity_type] += prob

        result_probs = sorted([(entity_type, prob/3) for entity_type, prob in type_probs.iteritems()],
                              key=lambda x: x[1], reverse=True)
        return result_probs


    def predict_types_full(self, context):
        """Context should always be a 5-element list
        :type type_hierarchy: DBPediaOntology
        """
        bigram_probs = self._get_ngram_probs(context)
        correct_types = [None]
        while True:
            parent = correct_types[-1]
            parent_probs = self._sum_probabilities(bigram_probs, parent)

            if len(parent_probs) == 0:
                break
            type_probs = defaultdict(lambda: 0)
            for probs in parent_probs:
                for entity_type, prob in probs:
                    type_probs[entity_type] += prob
            correct_types.append(max(type_probs.items(), key=lambda x: x[1])[0])

            # checks how often the selected type in not on top of n-gram separate ranks
            if correct_types[-1] not in [max(probs, key=lambda x: x[1])[0] for probs in parent_probs]:
                print parent_probs

        return list(reversed(correct_types[1:]))

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

