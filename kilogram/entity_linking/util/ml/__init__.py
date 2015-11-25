class Feature(object):
    graph_score = 0
    prior_prob = 0
    type_prob = 0
    type_match = 0
    type_exists = 0
    context_sim = 0
    cand_num = 0

    def __init__(self, candidate, entity, graph_score):
        self.graph_score = graph_score
        self.cand_num = len(candidate.entities)
        self.prior_prob = entity.count/sum([e.count for e in candidate.entities], 1)
        if entity.types:
            self.type_exists = 1
            self.type_match = int(candidate.type == entity.get_generic_type())

