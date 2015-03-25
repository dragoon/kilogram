from __future__ import division
from collections import defaultdict
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


def predict_types(context, type_hierarchy):
    """Context should always be a 5-element list"""
    # pre, post, mid bigrams
    context[2] = SUBSTITUTION_TOKEN
    bigrams = [context[:3], context[-3:], context[1:4]]
    types = []
    for bigram in bigrams:
        type_values = NgramService.hbase_raw("ngram_types", " ".join(bigram), "ngram:value")
        if type_values:
            types.append(ListPacker.unpack(type_values))
    totals = [sum(int(x) for x in zip(*type_values)[1]) for type_values in types]
    bigram_probs = [[(entity_type, int(count)/totals[i]) for entity_type, count in type_values] for i, type_values in enumerate(types)]
    from scipy.stats import entropy
    # filter by entropy
    bigram_probs = [probs for probs in bigram_probs if entropy(zip(*probs)[1]) < 3]
    type_probs = defaultdict(lambda: 0)
    for probs in bigram_probs:
        for entity_type, prob in probs:
            type_probs[entity_type] += prob

    result_probs = sorted([(entity_type, prob/3) for entity_type, prob in type_probs.iteritems()],
                          key=lambda x: x[1], reverse=True)
    return result_probs
    result_probs_dict = dict(result_probs)

    correct_types = []

    while type_hierarchy.get(result_probs[0][0], None):
        parent = result_probs.pop(0)[0]
        correct_types.append(parent)
        # get probability from dict, it could be updated
        parent_prob = result_probs_dict[parent]
        del result_probs_dict[parent]

        existing_children = [child for child in type_hierarchy[parent] if child in result_probs_dict]
        for child in existing_children:
            result_probs_dict[child] += parent_prob/len(existing_children)

    residual_types = zip(*sorted(result_probs_dict.items(), key=lambda x: x[1], reverse=True))[0]
    return correct_types + list(residual_types)

