from __future__ import division


class Feature(object):
    graph_score = 0
    prior_prob = 0
    type_prob = 0
    type_pmi = 0
    ner_type_match = 0
    true_type_match = 0
    type_exists = 0
    type_predictable = 0
    context_sim = 0
    cand_num = 0
    type_prob_rank = None
    prior_prob_rank = None
    number_merged = 1
    label = 0
    uri = None

    def __init__(self, candidate, entity, graph_rank, label):
        self.uri = entity.uri
        try:
            true_entity = [e for e in candidate.entities if e.uri == candidate.truth_data['uri']][0]
        except IndexError:
            true_entity = None

        self.label = label
        self.graph_score = graph_rank
        self.cand_num = len(candidate.entities)
        self.prior_prob = entity.count/sum([e.count for e in candidate.entities], 1)
        if entity.types:
            self.type_exists = 1
            self.ner_type_match = int(candidate.type == entity.get_generic_type())
            if candidate.context_types:
                type_probs = []
                if true_entity and true_entity.types:
                    self.true_type_match = int(entity.get_generic_type() in true_entity.types)
                for order, values in candidate.context_types.items():
                    for ngram_value in values:
                        for type_obj in ngram_value['values']:
                            if type_obj['name'] in entity.types:
                                type_probs.append(type_obj['prob'])

                # select max type prob
                if type_probs:
                    self.type_predictable = 1
                    self.type_prob = max(type_probs)

    @staticmethod
    def header():
        return 'URI\tCAND_NUM\tGRAPH_SCORE\tDBP_TYPE_EXISTS\tNER_TYPE_MATCH\tTRUE_TYPE_MATCH' \
               '\tTYPE_PREDICTABLE\tTYPE_PROB\tTYPE_PROB_RANK\tNUMBER_MERGED\tPRIOR_PROB\tPRIOR_PROB_RANK' \
               '\tLABEL'

    def __str__(self):
        return '\t'.join((str(x) for x in (self.uri, self.cand_num, self.graph_score,
                                           self.type_exists, self.ner_type_match,
                                           self.true_type_match,
                                           self.type_predictable, self.type_prob,
                                           self.type_prob_rank,
                                           self.number_merged,
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
        result_f.number_merged = len(features)
        return result_f

    @staticmethod
    def add_candidate_features(features):
        """
        Add candidate-level features (ranks) to existing features
        :param features: features for one mention
        :return: feature list
        """

        for i, feature in enumerate(sorted(features, key=lambda f: f.type_prob, reverse=True)):
            if feature.type_prob == 1.0:
                # more than 1 zero rank is allowed
                feature.type_prob_rank = 0
            else:
                feature.type_prob_rank = i
        for i, feature in enumerate(sorted(features, key=lambda f: f.prior_prob, reverse=True)):
            feature.prior_prob_rank = i
        return features
