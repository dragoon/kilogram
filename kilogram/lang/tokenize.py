import re

INITIAL_REGEX = re.compile(r'[A-Z]\.')
MULTIPLE_PUNCT_REGEX = re.compile(r'([.!?]){2,}')
MULTIPLE_SPACE_REGEX = re.compile(r'([ ]){2,}')
GARBAGE_REGEX = re.compile(r'[^\w\s]')

_DOT_TOKENS = {'mr.', 'dr.', 'ltd.', 'i.e.', 'u.s.', 'u.s.a.', 'e.g.', 'ft.', 'cf.', 'etc.', 'al.'}
_TIME_TOKENS = {'a.', 'p.', 'm.', 'a.m.', 'p.m.', 'p.m', 'a.m'}

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

            # concatenate a.m./p.m. for simpler time parsing
            if token in _TIME_TOKENS and len(all_tokens) > 0:
                all_tokens[-1] += token
                return

            if token.lower() in _DOT_TOKENS or INITIAL_REGEX.match(token):
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


if __name__ == '__main__':
    # some tests
    text = 'non-violence (anarcho-pacifism), while others'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'non-violence ( anarcho-pacifism ) , while others'

    text = 'reclassic and Classic (roughly 500 BC to 800 AD). The site was the capital of a Maya city-state located'
    tokens = default_tokenize_func(text)
    assert ' '.join(tokens) == 'reclassic and Classic ( roughly 500 BC to 800 AD ) . The site was the capital of a Maya city-state located'
