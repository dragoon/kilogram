import re
from ...dataset.dbpedia import NgramEntityResolver
from ...lang.tokenize import default_tokenize_func, tokenize_possessive, wiki_tokenize_func
from ...dataset.edit_histories.wikipedia import line_filter


ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>')


def get_unambiguous_labels(filename):
    unambiguous_labels = {}
    for line in open(filename, 'r'):
        label, uri = line.strip().split('\t')
        unambiguous_labels[label] = uri
    return unambiguous_labels


dbp_labels = {}
for line in open("dbpedia_data.txt"):
    entity, entity_types, redirects = line.split('\t')
    if len(entity_types.strip()):
        label = ' '.join(tokenize_possessive(default_tokenize_func(entity.replace("_", " "))))
        dbp_labels[label] = entity

del dbp_labels['.']

ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_2015-04.owl")


def generate_organic_precise_plus(line, evaluator=None):
    organic_precise = set()
    for token, uri, sentence in generate_organic_links(line):
        if token.lower().replace(' ', '_') in uri.lower():
            organic_precise.add((token, uri, sentence))
            yield token, uri, sentence
    if evaluator:
        for token, uri, sentence in evaluator(line):
            if (token, uri, sentence) not in organic_precise:
                yield token, uri, sentence


def generate_organic_plus(line, evaluator=None):
    organic_precise = set()
    for token, uri, sentence in generate_organic_links(line):
        organic_precise.add((token, uri, sentence))
        yield token, uri, sentence
    if evaluator:
        for token, uri, sentence in evaluator(line):
            if (token, uri, sentence) not in organic_precise:
                yield token, uri, sentence


def generate_organic_links(line):
    line = line.strip()
    for sentence in line_filter(' '.join(tokenize_possessive(wiki_tokenize_func(line)))):
        sentence_plain = ENTITY_MATCH_RE.sub('\g<2>', sentence).replace('_', ' ')
        for match in ENTITY_MATCH_RE.finditer(sentence):
            uri = match.group(1)
            uri = uri[0].upper() + uri[1:]
            uri = ner.redirects_file.get(uri, uri)
            if not uri in ner.dbpedia_types:
                continue

            anchor = match.group(2).replace('_', ' ')
            yield anchor, uri, sentence_plain


def generate_links(line, generators=None):
    """
    :param generators: list of functions generating uris from tokens
    :return:
    """
    if generators is None:
        generators = []
    line = line.strip()
    line = ENTITY_MATCH_RE.sub('\g<2>', line).replace('_', ' ')
    for sentence in line_filter(' '.join(tokenize_possessive(default_tokenize_func(line)))):
        sentence = sentence.split()
        i = 0
        while i < len(sentence):
            for j in range(min(len(sentence), i+20), i, -1):
                token = ' '.join(sentence[i:j])
                if i+1 == j and i == 0:
                    # if first word in sentence -> do not attempt to link, could be wrong (Apple)
                    continue
                # check token doesn't span titles
                elif j + 1 < len(sentence) and sentence[j][0].isupper():
                    continue
                # check token doesn't span titles
                elif i > 0 and sentence[i-1][0].isupper():
                    continue
                else:
                    match = False
                    for link_generator in generators:
                        uri, new_token = link_generator(token)
                        if uri:
                            yield new_token, uri, ' '.join(sentence)
                            i = j-1
                            match = True
                            break
                    if match:
                        break
            i += 1


def unambig_generator(token, unambiguous_labels=None):
    if token in unambiguous_labels:
        uri = unambiguous_labels[token]
        # prefer non-possessives
        if token.endswith("'s"):
            token = token[:-3]
        return uri, token
    return None, None


def label_generator(token):
    if token in dbp_labels:
        uri = dbp_labels[token]
        return uri, token
    return None, None
