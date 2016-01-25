from __future__ import division

import nltk
from .densest_subgraph import SemanticGraph
from .. import extract_candidates
from kilogram.lang.tokenize import default_tokenize_func


def link(sentence):
    tokens = default_tokenize_func(sentence)
    pos_tokens = nltk.pos_tag(tokens)
    candidates = extract_candidates(pos_tokens)
    if len(candidates) > 0:
        graph = SemanticGraph(candidates)
        graph.do_iterative_removal()
        graph.do_linking()
    return candidates
