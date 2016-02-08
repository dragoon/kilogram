from __future__ import division
from collections import defaultdict
import nltk
from kilogram import ListPacker, NgramService
from kilogram.lang import split_camel_case

PERCENTILE = 0.9


class Entity:
    uri = None
    count = 0
    types = None

    def __init__(self, uri, count, ner):
        self.uri = uri
        self.count = count
        if ner is not None:
            try:
                self.types = ner.get_types(uri)
            except KeyError:
                pass

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
    context_types = None
    has_super = None

    def _get_uri_counts(self):
        table = "wiki_anchor_ngrams"
        column = "ngram:value"
        prev_cand_string = None
        for cand_string in (self.cand_string, self.cand_string.title(), split_camel_case(self.cand_string)):
            if prev_cand_string and cand_string == prev_cand_string:
                continue
            res = NgramService.hbase_raw(table, cand_string, column)
            if res:
                candidates = ListPacker.unpack(res)
                self.cand_string = cand_string
                return [(uri, float(count)) for uri, count in candidates]
            prev_cand_string = cand_string

        res = NgramService.hbase_raw("wiki_anchor_ngrams_nospace",
                                     self.cand_string.replace(' ', '').lower(), column)
        if res:
            candidates = ListPacker.unpack(res)
            self.cand_string = self.cand_string.replace(' ', '').lower()
            return [(uri, float(count)) for uri, count in candidates]

        return None

    def init_context_types(self, type_predictor):
        if self.context:
            self.context_types = type_predictor.predict_types_features(self.context.split())

    def _init_entities(self, uri_counts, ner):
        self.entities = []
        for uri, count in uri_counts.items():
            self.entities.append(Entity(uri, count, ner))

    def __init__(self, start_i, end_i, cand_string, noun_index=None, e_type=None,
                 context=None, ner=None, candidates=None):
        self.type = e_type
        self.context = context
        self.cand_string = cand_string
        self.start_i = start_i
        self.end_i = end_i
        self.noun_index = noun_index
        self.entities = []
        self.has_super = False
        uri_counts = {}

        # take Xs percentile to remove noisy candidates
        if candidates:
            temp_candidates = candidates
        else:
            temp_candidates = self._get_uri_counts()
        if temp_candidates is None:
            return
        total_c = sum(zip(*temp_candidates)[1])
        cur_c = 0
        # TODO: percentile has impact on the number of candidates and on the heuristic respectively
        for uri, count in sorted(temp_candidates, key=lambda x: x[1], reverse=True):
            if cur_c/total_c > PERCENTILE:
                break
            cur_c += count
            uri_counts[uri] = count
        # also remove all counts = 1
        # TODO: do experiments
        if any(uri_counts.values()) >= 2:
            for uri in uri_counts.keys():
                if uri_counts[uri] < 2:
                    del uri_counts[uri]
        self._init_entities(uri_counts, ner)

    def _prune_generic_types(self):
        true_entity = None
        for entity in self.entities:
            if self.truth_data and entity.uri == self.truth_data['uri']:
                true_entity = entity

        if not true_entity:
            return

        if true_entity.types:
            self.entities = [e for e in self.entities if e.types is None or e.get_generic_type() == true_entity.get_generic_type()]

    def _prune_specific_types(self):
        true_entity = None
        for entity in self.entities:
            if self.truth_data and entity.uri == self.truth_data['uri']:
                true_entity = entity

        if not true_entity:
            return

        if true_entity.types:
            self.entities = [e for e in self.entities if e.types is None or e.types[0] in true_entity.types]

    def get_max_uri(self):
        if not self.entities:
            return None
        return max(self.entities, key=lambda e: e.count).uri

    def get_max_entity(self):
        if not self.entities:
            return None
        return max(self.entities, key=lambda e: e.count)

    def get_max_typed_uri(self, context_types_list):
        if not context_types_list:
            return self.get_max_uri()

        type_probs = defaultdict(lambda: 0)
        for context_types in context_types_list:
            for order, values in context_types.items():
                for ngram_value in values:
                    for type_obj in ngram_value['values']:
                        type_probs[type_obj['name']] = max(type_probs[type_obj['name']], type_obj['prob'])

        if len(self.entities) > 1:
            for entity in sorted(self.entities, key=lambda e: e.count, reverse=True):
                try:
                    if type_probs[entity.get_generic_type()] > 0.90:
                        return entity.uri
                except TypeError:
                    break
        return self.get_max_uri()

    def __len__(self):
        return len(self.entities)

    def __repr__(self):
        return self.cand_string + ":" + str(self.resolved_true_entity)


def syntactic_subsumption(candidates):
    """replace candidates that are syntactically part of another: Ford -> Gerald Ford"""
    cand_dict = dict([(x.cand_string, x) for x in candidates])

    def get_super_candidates(c):
        res = []
        for cand_string in cand_dict.keys():
            if c.cand_string != cand_string and c.cand_string in cand_string:
                if cand_dict[cand_string].entities:
                    res.append(cand_dict[cand_string])
        return res
    for candidate in candidates:
        super_candidates = get_super_candidates(candidate)
        if super_candidates:
            person_cands = [x for x in super_candidates if x.type == '<dbpedia:Person>']
            if len(person_cands) >= 1:
                candidate.entities = person_cands[0].entities
            else:
                max_ent = candidate.get_max_entity()
                current_entity_set = set([e.uri for e in candidate.entities])
                candidate.entities = []
                for super_candidate in super_candidates:
                    candidate.entities.extend([e for e in super_candidate.entities if e.uri in current_entity_set])
                # add everything if still empty -- means no good matches
                if len(candidate.entities) == 0:
                    for super_candidate in super_candidates:
                        candidate.entities.extend(super_candidate.entities)
                if max_ent:
                    candidate.entities.append(max_ent)
                candidate.has_super = True
                if candidate.truth_data and candidate.truth_data['uri'] is not None and candidate.truth_data['uri'] not in [x.uri for x in candidate.entities]:
                    print('Not in truth!', candidate)


def closeness_pruning(candidates, pickle_dict=None):
    import itertools
    for cand1, cand2 in itertools.combinations(candidates, 2):
        prev_max_count = 0
        new_cand1_entities = []
        new_cand2_entities = []
        if cand1.cand_string == cand2.cand_string:
            continue
        if len(cand1.entities) > len(cand2.entities):
            cand2, cand1 = cand1, cand2
        cand2_entities = set([e.uri for e in cand2.entities])
        if not cand2_entities:
            continue
        for entity in cand1.entities:
            if entity.uri in pickle_dict:
                related_uris = pickle_dict[entity.uri]
            else:
                related_uris = NgramService.get_wiki_links_cooccur(entity.uri)
                pickle_dict[entity.uri] = related_uris
            intersect = set(related_uris.keys()).intersection(cand2_entities)
            if intersect:
                for intersect_uri in intersect:
                    pickle_dict[intersect_uri] = NgramService.get_wiki_links_cooccur(intersect_uri)
                    max_count = int(related_uris[intersect_uri]) + int(pickle_dict[intersect_uri].get(entity.uri, 0))
                    if prev_max_count < max_count:
                        new_cand1_entities = [entity]
                        new_cand2_entities = [e for e in cand2.entities if e.uri == intersect_uri]
                        prev_max_count = max_count
                    elif prev_max_count == max_count:
                        new_cand1_entities.append(entity)
                        new_cand2_entities.extend([e for e in cand2.entities if e.uri == intersect_uri])

        if new_cand1_entities:
            cand1.entities = new_cand1_entities
        if new_cand2_entities:
            cand2.entities = new_cand2_entities

    for cand in candidates:
        if cand.cand_string.islower() and len(cand.entities) > 1:
            cand.entities = []


def extract_candidates(pos_tokens):
    """
    :param pos_tokens: list of words annotated with POS tags
    :return:
    """

    entity_indexes = set()
    cand_entities = []

    # TODO: join noun sequences?
    def join_nps(pos_tokens):
        new_tokens = [list(pos_tokens[0])]
        is_nn = pos_tokens[0][1].startswith('NN')
        for word, pos_tag in pos_tokens[1:]:
            if is_nn and pos_tag.startswith('NN'):
                new_tokens[-1][1] += ' ' + pos_tag
                new_tokens[-1][0] += ' ' + word
            else:
                new_tokens.append([word, pos_tag])
                is_nn = pos_tag.startswith('NN')
        return new_tokens

    pos_tokens = join_nps(pos_tokens)

    noun_indexes = [i for i, word_token in enumerate(pos_tokens) if word_token[1].startswith('NN')]
    words = zip(*pos_tokens)[0]
    for noun_index in noun_indexes:
        n = 1
        while True:
            start_i = max(0, noun_index+1-n)
            end_i = min(len(words), noun_index+n)
            # whether to continue to expand noun phrase
            prev_len = len(cand_entities)
            for n_i, ngram in enumerate(nltk.ngrams(words[start_i:end_i], n)):
                ngram = ' '.join(ngram)
                cand_entity = CandidateEntity(start_i+n_i, start_i+n_i+n, ngram,
                                              noun_index=noun_index)
                # TODO: what to do with lower-case things?
                if not cand_entity.cand_string[0].isupper():
                    continue
                if cand_entity.entities and (cand_entity.start_i, cand_entity.end_i) not in entity_indexes:
                    entity_indexes.add((cand_entity.start_i, cand_entity.end_i))
                    cand_entities.append(cand_entity)
                # no uris? then check if sub-ngrams possible
                elif n == 1 and len(ngram.split()) > 1:
                    # concatenated nouns => split
                    ngram = ngram.split()
                    for i in range(len(ngram)-1, 0, -1):
                        for subngram in nltk.ngrams(ngram, i):
                            cand_entity = CandidateEntity(start_i, start_i+1, ' '.join(subngram),
                                                          noun_index=noun_index)
                            if cand_entity.entities:
                                cand_entities.append(cand_entity)
                        if len(cand_entities) > prev_len:
                            break
            if prev_len == len(cand_entities):
                # nothing was added => break
                break
            if start_i == 0 and end_i == len(words)-noun_index:
                break
            n += 1
    return cand_entities
