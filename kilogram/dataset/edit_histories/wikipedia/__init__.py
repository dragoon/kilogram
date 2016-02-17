import re

END_SENTENCE_RE = re.compile(r'\s[.?!]\s(?=[^a-z])')


def line_filter(line):
    sentences = END_SENTENCE_RE.split(line)
    last = len(sentences) - 1
    for i, sentence in enumerate(sentences):
        if not sentence.strip():
            continue
        if i == last and not sentence.endswith('.'):
            continue
        if not sentence.endswith('.'):
            sentence += ' .'
        yield sentence
