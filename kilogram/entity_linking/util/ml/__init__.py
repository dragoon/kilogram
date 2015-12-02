from __future__ import division


class Feature(object):
    graph_score = 0
    prior_prob = 0
    type_prob = 0
    type_pmi = 0
    type_match = 0
    type_exists = 0
    type_predictable = 0
    context_sim = 0
    cand_num = 0
    type_prob_rank = None
    prior_prob_rank = None
    label = 0

    def __init__(self, candidate, entity, graph_rank, label):
        self.label = label
        self.graph_score = graph_rank
        self.cand_num = len(candidate.entities)
        self.prior_prob = entity.count/sum([e.count for e in candidate.entities], 1)
        if entity.types:
            self.type_exists = 1
            self.type_match = int(candidate.type == entity.get_generic_type())
            if candidate.context_types:
                pre_bigrams = [x for x in candidate.context_types[2][0]['values'] if x['name'] in entity.types]
                if pre_bigrams:
                    self.type_predictable = 1
                    self.type_prob = pre_bigrams[0]['prob']

    @staticmethod
    def header():
        return 'CAND_NUM\tGRAPH_SCORE\tTYPE_EXISTS\tTYPE_MATCH\tTYPE_PREDICTABLE\tTYPE_PROB\tTYPE_PROB_RANK\tPRIOR_PROB\tPRIOR_PROB_RANK' + \
               '\tLABEL'

    def __str__(self):
        return '\t'.join((str(x) for x in (self.cand_num, self.graph_score,
                                           self.type_exists, self.type_match,
                                           self.type_predictable, self.type_prob,
                                           self.type_prob_rank,
                                           self.prior_prob, self.prior_prob_rank,
                                           self.label)))

    @staticmethod
    def max_merge(features):
        if len(features) == 1:
            return features[0]
        max_type_prob = max(f.type_prob for f in features)
        max_pmi = max(f.type_pmi for f in features)
        result_f = features[0]
        result_f.type_prob = max_type_prob
        result_f.type_pmi = max_pmi
        return result_f

    @staticmethod
    def add_candidate_features(features):
        """
        Add candidate-level features (ranks) to existing features
        :param features: features for one mention
        :return: feature list
        """

        for i, feature in enumerate(sorted(features, key=lambda f: f.type_prob, reverse=True)):
            feature.type_prob_rank = i
        for i, feature in enumerate(sorted(features, key=lambda f: f.prior_prob, reverse=True)):
            feature.type_prob_rank = i
        return features
