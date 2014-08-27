import re
from ...lang import number_replace
from ...lang.tokenize import wiki_tokenize_func


ENTITY_MATCH_RE = re.compile(r'<(.+)\|(.+)>')


def parse_types_text(text, dbpedia_types, numeric=True):
    """
    :type dbpedia_types: dict
    :type dbpedia_redirects: dict
    """
    line = wiki_tokenize_func(text)
    new_line = []
    for word in line:
        match = ENTITY_MATCH_RE.search(word)
        if match:
            uri = match.group(1)
            orig_text = match.group(2)
            orig_text = orig_text.replace('_', ' ')
            stop = False
            for uri in (uri, uri.capitalize()):
                if uri in dbpedia_types:
                    new_line.append(match.expand('<dbpedia:' + dbpedia_types['types'][uri][0]+'>'))
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
