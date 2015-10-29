from .. import parse_candidate
from kilogram import NgramService


def get_max_uri(text, table="wiki_anchor_ngrams"):
    try:
        candidates = parse_candidate(NgramService.hbase_raw(table, text, "ngram:value"))
        if not candidates:
            candidates = parse_candidate(NgramService.hbase_raw(table, text.title(), "ngram:value"))
        return max(candidates, key=lambda x: x[1])[0]
    except:
        return None


def get_max_typed_uri(text, e_type, ner, table="wiki_anchor_ngrams"):
    for mention in (text, text.title()):
        candidates = parse_candidate(NgramService.hbase_raw(table, mention, "ngram:value"))
        for cand_uri, _ in sorted(candidates, key=lambda x: x[1], reverse=True):
            try:
                cand_type = ner.get_type(cand_uri, -1)
                if cand_type == e_type:
                    return cand_uri
            except IndexError:
                return cand_uri
    return None

