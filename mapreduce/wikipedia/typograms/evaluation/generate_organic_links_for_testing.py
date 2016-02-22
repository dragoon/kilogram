import sys
import re
from kilogram.lang.tokenize import default_tokenize_func
from kilogram.dataset.edit_histories.wikipedia import line_filter


ENTITY_MATCH_RE = re.compile(r'<([^\s]+?)\|([^\s]+?)>')


# Split each line into words
def generate_ngrams(line):
    line = line.strip()
    for sentence in line_filter(' '.join(default_tokenize_func(line))):
        for match in ENTITY_MATCH_RE.finditer(sentence):
            uri = match.group(0)
            anchor = match.group(1).replace('_', ' ')
            print(anchor.encode('utf-8') + '\t' + uri.encode('utf-8') + '\t' + ' '.join(sentence))

for line in sys.stdin:
    generate_ngrams(line)
