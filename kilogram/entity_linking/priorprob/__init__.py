from .. import parse_candidate
from kilogram import NgramService


def get_max_uri(text, table="wiki_anchor_ngrams"):
    try:
        return max(parse_candidate(NgramService.hbase_raw(table, text, "ngram:value")), key=lambda x: x[1])[0]
    except:
        return None
