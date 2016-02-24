import sys
import re
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.lang.tokenize import wiki_tokenize_func, tokenize_possessive
from kilogram.dataset.edit_histories.wikipedia import line_filter


ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>')
ner = NgramEntityResolver("dbpedia_data.txt", "dbpedia_2015-04.owl")


# Split each line into words
def generate_ngrams(line):
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
            print(anchor + '\t' + uri + '\t' + sentence_plain)

for line in sys.stdin:
    generate_ngrams(line)
