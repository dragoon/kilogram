import re

FLOAT_REGEX = r'\.\d+'
NUM_RE = re.compile(FLOAT_REGEX)

INITIAL_REGEX = re.compile(r'[A-Z]\.')
MULTIPLE_PUNCT_REGEX = re.compile(r'([.!?]){2,}')
MULTIPLE_SPACE_REGEX = re.compile(r'([ ]){2,}')
GARBAGE_REGEX = re.compile(r'[^\w\s]')

_DOT_TOKENS = {'i.e.', 'u.s.', 'u.s.a.', 'e.g.', 'ft.', 'cf.', 'etc.', 'approx.',
               'al.', 'no.', 'vs.', 'v.', 'op.',
               'mr.', 'ms.', 'mrs.', 'dr.', 'ph.d.', 'st.', 'jr.', 'sr.',  # people
               'sgt.', 'lt.', 'maj.', 'col.', 'gen.',  # military
               'a.m.', 'p.m.',  # time
               'corp.', 'inc.', 'ltd.', 'bros.', 'co.'}  # organizations

_SIMPLE_PUNCT = set('!"()*,:;<=>?[]{}.?')
_SIMPLE_PUNCT_WIKI = set('!"()*,:;=?[]{}.?')


def wiki_tokenize_func(sentence, punct_set=_SIMPLE_PUNCT_WIKI):
    return default_tokenize_func(sentence, punct_set)


def default_tokenize_func(sentence, punct_set=_SIMPLE_PUNCT):
    """
    Simple tokenization trying to keep all non-words like numbers, dates,
    times, website names, etc.
    """
    orig_tokens = sentence.strip().split()
    new_tokens = []
    for orig_token in orig_tokens:
        tokens = []

        def parse_token(all_tokens, token):
            if len(token) == 0:
                return
            to_append = []

            if token.lower() in _DOT_TOKENS or INITIAL_REGEX.match(token) or NUM_RE.match(token):
                all_tokens.append(token)
                return

            if token[0] in punct_set:
                all_tokens.append(token[0])
                parse_token(all_tokens, token[1:])
            elif token[-1] in punct_set:
                to_append = [token[-1]]
                parse_token(all_tokens, token[:-1])
            else:
                all_tokens.append(token)
            all_tokens.extend(to_append)

        parse_token(tokens, orig_token)
        new_tokens.extend(tokens)
    return new_tokens


def generate_possible_splits(words, max_seq_len=None):
    if max_seq_len is None:
        max_seq_len = len(words)
    for i in range(1, min(max_seq_len, len(words))):
        if len(words) - i <= 5:
            yield [words[:i], words[i:]]
        if len(words[i:]) > 1:
            for perm in generate_possible_splits(words[i:], max_seq_len):
                yield [words[:i]] + perm


if __name__ == '__main__':
    # some tests
    text = 'non-violence (anarcho-pacifism), while others'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'non-violence ( anarcho-pacifism ) , while others'

    text = 'reclassic and Classic (roughly 500 BC to 800 AD). The site was the capital of a Maya city-state located'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'reclassic and Classic ( roughly 500 BC to 800 AD ) . The site was the capital of a Maya city-state located'

    text = 'The same Latin stem gives rise to the terms a.m. (ante meridiem) and p.m. (post meridiem)'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'The same Latin stem gives rise to the terms a.m. ( ante meridiem ) and p.m. ( post meridiem )'

    text = 'Around .4 percent'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'Around .4 percent'
