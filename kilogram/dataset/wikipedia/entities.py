import re
from ...lang.tokenize import wiki_tokenize_func


ENTITY_MATCH_RE = re.compile(r'<(.+?)\|(.+?)>')


def parse_types_text(text, dbpedia_types):
    """
    :type dbpedia_types: dict
    """
    new_line = []
    new_line_plain = []
    for word in text.split():
        res = None
        res1 = None
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            orig_text = match.group(2)
            orig_text = orig_text.replace('_', ' ')
            for uri in (uri, uri.capitalize()):
                if uri in dbpedia_types:
                    types = dbpedia_types[uri]
                    res = [','.join(['<dbpedia:' + entity_type+'>' for entity_type in types])]
                    # TODO: check if we really should split
                    res1 = wiki_tokenize_func(orig_text)
                    break
            if not res:
                res = wiki_tokenize_func(orig_text)
        else:
            res = [word]
        new_line.extend(res)
        if res1:
            res = res1
        new_line_plain.extend(res)
    return new_line, new_line_plain
