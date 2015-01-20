import re
from ...lang import number_replace
from ...lang.tokenize import wiki_tokenize_func


ENTITY_MATCH_RE = re.compile(r'<(.+)\|(.+)>')
SIMPLE_TYPES = set(['Place', 'Person', 'Organisation'])


def parse_types_text(text, dbpedia_types, numeric=False, type_level=-1, type_filter=None):
    """
    :type dbpedia_types: dict
    """
    new_line = []
    for word in text.split():
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            orig_text = match.group(2)
            orig_text = orig_text.replace('_', ' ')
            stop = False
            for uri in (uri, uri.capitalize()):
                if uri in dbpedia_types:
                    types = dbpedia_types[uri]
                    dbp_type = types[type_level]
                    if type_filter and dbp_type not in type_filter:
                        continue
                    new_line.append(match.expand('<dbpedia:' + dbp_type+'>'))
                    stop = True
                    break
            if not stop:
                if numeric:
                    new_line.extend([number_replace(x) for x in wiki_tokenize_func(orig_text)])
                else:
                    new_line.extend(wiki_tokenize_func(orig_text))

        elif numeric:
            new_line.append(number_replace(word))
        else:
            new_line.append(word)
    return ' '.join(new_line)
