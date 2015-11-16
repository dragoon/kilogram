from kilogram import ListPacker, NgramService

PERCENTILE = 0.9

class CandidateEntity:
    uri_counts = None
    start_i = 0
    end_i = 0
    noun_index = 0
    cand_string = None
    resolved_true_entity = None
    e_type = None
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

    def __init__(self, start_i, end_i, cand_string, noun_index=None, e_type=None, context=None):
        self.e_type = e_type
        self.context = context
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        self.noun_index = noun_index
        self.uri_counts = {}

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
            self.uri_counts[uri] = count
        # also remove all counts = 1
        # TODO: do experiments
        for uri in self.uri_counts.keys():
            if self.uri_counts[uri] < 2:
                del self.uri_counts[uri]

    def keep_only_types(self, ner, types_to_keep):
        for uri in self.uri_counts.keys():
            try:
                uri_type = ner.get_type(uri, -1)
                if uri_type not in types_to_keep:
                    del self.uri_counts[uri]
            except KeyError:
                del self.uri_counts[uri]

    def prune_types(self, e_type, ner):
        if self.uri_counts and e_type:
            for uri in self.uri_counts.keys():
                try:
                    uri_type = ner.get_type(uri, -1)
                    if uri_type != e_type:
                        del self.uri_counts[uri]
                except KeyError:
                    pass

    def get_max_uri(self):
        if not self.uri_counts:
            return None
        return max(self.uri_counts.items(), key=lambda x: x[1])[0]

    def get_max_typed_uri(self, ner):
        for cand_uri, _ in sorted(self.uri_counts.items(), key=lambda x: x[1], reverse=True):
            try:
                cand_type = ner.get_type(cand_uri, -1)
                if cand_type == self.e_type:
                    return cand_uri
            except KeyError:
                return cand_uri
        return None

    def __len__(self):
        return len(self.uri_counts)

    def __repr__(self):
        return self.cand_string + ":" + str(self.resolved_true_entity)


def syntactic_subsumption(candidates):
    """replace candidates that are syntactically part of another: Ford -> Gerald Ford"""
    cand_dict = dict([(x.cand_string, x) for x in candidates])

    def get_super_candidate(candidate):
        for cand_string in cand_dict.keys():
            if candidate.cand_string != cand_string and candidate.cand_string in cand_string:
                return cand_dict[cand_string]
        return None
    for candidate in candidates:
        super_candidate = get_super_candidate(candidate)
        if super_candidate:
            candidate.uri_counts = super_candidate.uri_counts
