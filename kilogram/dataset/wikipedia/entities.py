import re
from ...lang import number_replace
from ...lang.tokenize import wiki_tokenize_func


ENTITY_MATCH_RE = re.compile(r'<(.+)\|(.+)>')


def parse_types_text(text, dbpedia_redirects, dbpedia_types):
    """
    :type dbpedia_types: dict
    :type dbpedia_redirects: dict
    """
    line = wiki_tokenize_func(text)
    for i, word in enumerate(line):
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            orig_text = match.group(2)
            orig_text = orig_text.replace('_', ' ')
            for uri in (uri, uri.capitalize()):
                stop = False
                if uri in dbpedia_types:
                    line[i] = match.expand('<dbpedia:' + dbpedia_types[uri][0]+'>')
                    stop = True
                elif uri in dbpedia_redirects and dbpedia_redirects[uri] in dbpedia_types:
                    line[i] = match.expand('<dbpedia:' + dbpedia_types[dbpedia_redirects[uri]][0] + '>')
                    stop = True
                else:
                    line[i] = number_replace(orig_text)
                if stop:
                    break
        else:
            line[i] = number_replace(line[i])
    return ' '.join(line)