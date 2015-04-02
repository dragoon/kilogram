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


class TypePredictor(object):
    type_hierarchy = None
    hbase_table = None
    dbpedia_types_db = None
    type_priors = None
    word2vec_model = None

    def __init__(self, hbase_table, type_hierarchy=None, dbpedia_types_db=None, word2vec_model_filename=None):
        self.type_hierarchy = type_hierarchy
        self.hbase_table = hbase_table
        if dbpedia_types_db:
            self.dbpedia_types_db = shelve.open(dbpedia_types_db, flag='r')
        self.type_priors = {}
        total = sum(NgramService.substitution_counts.values())
        for entity_type, count in NgramService.substitution_counts.items():
            self.type_priors[entity_type] = count/total

        if word2vec_model_filename:
            from gensim.models.word2vec import Word2Vec
            self.word2vec_model = Word2Vec.load(word2vec_model_filename)

    def _predict_types_from_ngram(self, ngram):
        types_ranked = []
        for entity_type in self.type_priors.keys():
            score = self.word2vec_model.similarity(ngram, entity_type)
            types_ranked.append((entity_type, score))
        types_ranked.sort(key=lambda x: x[1])
        return types_ranked

    def _resolve_entities(self, context):
        entity_idx = [i for i, x in enumerate(context) if x.startswith('<URI:')]
        if len(entity_idx) > 0:
            for entity_ind in entity_idx:
                uri = context[entity_ind][5:-1].encode('utf8')
                if uri in self.dbpedia_types_db:
                    types = self.dbpedia_types_db[uri]
                    # most specific
                    context[entity_ind] = '<dbpedia:' + types[0] + '>'
                else:
                    context[entity_ind] = context[entity_ind][5:-1].replace('_', ' ').split()[0]
        return context

    def _get_ngram_probs(self, context):
        # pre, post, mid bigrams
        context[2] = SUBSTITUTION_TOKEN
        context = self._resolve_entities(context)
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
                type_probs[entity_type] += prob/len(bigram_probs)
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