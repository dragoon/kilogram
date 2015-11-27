from kilogram import ListPacker, NgramService

PERCENTILE = 0.9


class Entity:
    uri = None
    count = 0
    types = None

    def __init__(self, uri, count, ner):
        self.uri = uri
        self.count = count
        if ner is not None:
            self.types = ner.get_types(uri)

    def get_generic_type(self):
        return self.types[-1]

    def __repr__(self):
        return self.uri + ': ' + str(self.count)


class CandidateEntity:
    entities = None
    start_i = 0
    end_i = 0
    noun_index = 0
    cand_string = None
    resolved_true_entity = None
    type = None
    truth_data = None
    context = None

    def _get_uri_counts(self):
        table = "wiki_anchor_ngrams"
        column = "ngram:value"
        res = NgramService.hbase_raw(table, self.cand_string, column)
        if not res:
            res = NgramService.hbase_raw(table, self.cand_string.title(), column)
        if res:
            candidates = ListPacker.unpack(res)
            return [(uri, long(count)) for uri, count in candidates]
        return None

    def _init_entities(self, uri_counts, ner):
        self.entities = []
        for uri, count in uri_counts.items():
            self.entities.append(Entity(uri, count, ner))

    def __init__(self, start_i, end_i, cand_string, noun_index=None, e_type=None, context=None, ner=None):
        self.type = e_type
        self.context = context
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        self.noun_index = noun_index
        self.entities = []
        uri_counts = {}

        # take Xs percentile to remove noisy candidates
        temp_candidates = self._get_uri_counts()
        if temp_candidates is None:
            return
        total_c = sum(zip(*temp_candidates)[1])
        cur_c = 0
        for uri, count in sorted(temp_candidates, key=lambda x: x[1], reverse=True):
            if cur_c/total_c > PERCENTILE:
                break
            cur_c += count
            uri_counts[uri] = count
        # also remove all counts = 1
        # TODO: do experiments
        for uri in uri_counts.keys():
            if uri_counts[uri] < 2:
                del uri_counts[uri]
        self._init_entities(uri_counts, ner)

    def get_max_uri(self):
        if not self.entities:
            return None
        return max(self.entities, key=lambda e: e.count).uri

    def get_max_typed_uri(self):
        for entity in sorted(self.entities, key=lambda e: e.count, reverse=True):
            try:
                if entity.get_generic_type() == self.type:
                    return entity.uri
            except KeyError:
                return entity.uri
        return None

    def __len__(self):
        return len(self.entities)

    def __repr__(self):
        return self.cand_string + ":" + str(self.resolved_true_entity)


def syntactic_subsumption(candidates):
    """replace candidates that are syntactically part of another: Ford -> Gerald Ford"""
    cand_dict = dict([(x.cand_string, x) for x in candidates])

    def get_super_candidate(c):
        for cand_string in cand_dict.keys():
            if c.cand_string != cand_string and c.cand_string in cand_string:
                if cand_dict[cand_string].type == '<dbpedia:Person>':
                    return cand_dict[cand_string]
        return None
    for candidate in candidates:
        super_candidate = get_super_candidate(candidate)
        if super_candidate:
            candidate.entities = super_candidate.entities
