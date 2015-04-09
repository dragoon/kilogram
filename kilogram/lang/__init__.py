# coding=utf-8
import re
import socket
import time

DT_STRIPS = {'my', 'our', 'your', 'their', 'a', 'an', 'the', 'her', 'its', 'his'}
PUNCT_SET = set('[!(),.:;?/[\\]^`{|}]')

FLOAT_REGEX = r'-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?'
INT_REGEX = r'\b[1-9][\d,]*\b'

FAST_NUM_REGEX = re.compile(r'\d')

INT_RE = re.compile(INT_REGEX)
PERCENT_RE = re.compile(r'\b{0}%'.format(FLOAT_REGEX))
NUM_RE = re.compile(FLOAT_REGEX)
TIME_RE1 = re.compile(r'\b\d{1,2}:\d{2}\b')
TIME_RE2 = re.compile(r'\b\d{1,2}(?:[:\.][0-5]\d)?\s?(a\.m\.|p\.m\.|am|pm)\b')

VOL_RE = re.compile(r'\b{0}\s?(m3|cubic \w+)\b'.format(FLOAT_REGEX))
SQ_RE = re.compile(r'\b{0}\s?(m2|square \w+)\b'.format(FLOAT_REGEX))
GEO_RE = re.compile(r"{0}° {1}'".format(INT_REGEX, INT_REGEX))
TEMPERATURE_RE = re.compile(r'{0}\s?(°C|Celsius|°F|Fahrenheit)'.format(FLOAT_REGEX))

_RE_NUM_SUBS = [('<NUM:AREA>', SQ_RE), ('<NUM:VOL>', VOL_RE), ('<NUM:GEO>', GEO_RE),
                ('<NUM:TEMP>', TEMPERATURE_RE), ('<NUM:PERCENT>', PERCENT_RE),
                ('<DATE:TIME>', TIME_RE1), ('<DATE:TIME>', TIME_RE2), ('<NUM:INT>', INT_RE)]

NE_TOKEN = re.compile(r'<[A-Z]+>')
NE_END_TOKEN = re.compile(r'</[A-Z]+>$')


def number_replace(sentence):
    for repl, regex in _RE_NUM_SUBS:
        if not FAST_NUM_REGEX.search(sentence):
            break
        sentence = regex.sub(repl, sentence)
    return sentence

def strip_determiners(ngram):
    """
    :type ngram: unicode
    :return: n-gram with stripped determiners
    """
    ngram = ngram.split()
    dt_positions = [i for i, x in enumerate(ngram) if x.lower() in DT_STRIPS]
    new_ngram = [word for i, word in enumerate(ngram)
                 if i not in dt_positions or
                 (i+1 < len(ngram) and ngram[i+1] in PUNCT_SET)]
    return ' '.join(new_ngram)


def strip_adjectives(tokens, pos_tokens):
    """
    :type tokens: list
    :type pos_tokens: list
    :return: (tokens, pos_tokens) with stripped adjectives
    """
    new_tokens = []
    adj_tokens = []
    for token, pos_tag in zip(tokens, pos_tokens):
        if pos_tag == 'JJ':# or (adj_tokens and pos_tag == 'CC'):
            adj_tokens.append((token, pos_tag))
            continue
        elif pos_tag.startswith('NN') and adj_tokens:
            adj_tokens = []
        elif adj_tokens:
            new_tokens.extend(adj_tokens)
            adj_tokens = []
        new_tokens.append((token, pos_tag))
    if new_tokens:
        return zip(*new_tokens)
    else:
        return [], []


def _stanford_socket(hostname, port, content):
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((hostname, port))
            break
        except socket.error:
            time.sleep(1)
    s.sendall(content.encode('utf-8'))
    s.shutdown(socket.SHUT_WR)
    data = ""
    while 1:
        l_data = s.recv(8192)
        if l_data == "":
            break
        data += l_data
    s.close()
    return data


def pos_tag(sentence):
    from .. import ST_HOSTNAME, ST_PORT

    def compress_pos(pos_tag):
        if pos_tag.startswith('VB'):
            pos_tag = 'VB'
        elif pos_tag == 'NNS':
            pos_tag = 'NN'
        return pos_tag
    pos_tokens = _stanford_socket(ST_HOSTNAME, ST_PORT, sentence).strip()
    return [compress_pos(x.split('_')[1]) for x in pos_tokens.split()]


def replace_ne(sentence):
    """
    /usr/lib/jvm/java-8-oracle/bin/java -mx500m -cp stanford-ner.jar edu.stanford.nlp.ie.NERServer -port 9191 -outputFormat inlineXML -loadClassifier classifiers/english.muc.7class.distsim.crf.ser.gz &
    """
    from .. import NER_HOSTNAME, NER_PORT
    ne_tokens = _stanford_socket(NER_HOSTNAME, NER_PORT, sentence).strip()
    typed_tokens = []
    is_ne = False
    for ne_token in ne_tokens.split():
        match_start = NE_TOKEN.match(ne_token)
        match_end = NE_END_TOKEN.search(ne_token)
        if match_start:
            typed_tokens.append(match_start.group(0))
            is_ne = True
        if match_end:
            is_ne = False
            continue
        if not is_ne:
            typed_tokens.append(ne_token)
    return ' '.join(typed_tokens)
