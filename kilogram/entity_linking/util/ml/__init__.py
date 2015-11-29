from __future__ import division


class Feature(object):
    graph_score = 0
    prior_prob = 0
    type_prob = 0
    type_pmi = 0
    type_match = 0
    type_exists = 0
    context_sim = 0
    cand_num = 0
    label = 0

    def __init__(self, candidate, entity, graph_score, label):
        self.label = label
        self.graph_score = graph_score
        self.cand_num = len(candidate.entities)
        self.prior_prob = entity.count/sum([e.count for e in candidate.entities], 1)
        if entity.types:
            self.type_exists = 1
            self.type_match = int(candidate.type == entity.get_generic_type())
            if candidate.context_types:
                pre_bigrams = [x for x in candidate.context_types[2][0]['values'] if x['name'] in entity.types]
                if pre_bigrams:
                    self.type_prob = pre_bigrams[0]['prob']
                    self.type_pmi = pre_bigrams[0]['pmi']

    @staticmethod
    def header():
        return 'CAND_NUM\tGRAPH_SCORE\tTYPE_EXISTS\tTYPE_MATCH\tTYPE_PROB\tPRIOR_PROB\tTYPE_PMI' + \
               '\tLABEL'

    def __str__(self):
        return '\t'.join((str(x) for x in (self.cand_num, self.graph_score,
                                           self.type_exists, self.type_match, self.type_prob,
                                           self.prior_prob, self.type_pmi, self.label)))


def max_merge(features):
    if len(features) == 1:
        return features[0]
    max_type_prob = max(f.type_prob for f in features)
    max_pmi = max(f.type_pmi for f in features)
    result_f = features[0]
    result_f.type_prob = max_type_prob
    result_f.type_pmi = max_pmi
    return result_f

