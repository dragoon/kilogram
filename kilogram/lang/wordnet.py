from nltk import WordNetLemmatizer
from nltk.corpus import wordnet


class WordNetNgram():
    lmtzr = WordNetLemmatizer()

    def __init__(self):
        pass

    @classmethod
    def get_synonyms(cls, word, pos_tag):
        """Get ngram synonyms from WordNet"""
        word = cls.lemma(word, pos_tag)
        ss = wordnet.synsets(word, cls._get_wordnet_pos(pos_tag))
        synonyms = set([l.name for s in ss for l in s.lemmas]) - {word}
        return synonyms

    @classmethod
    def lemma(cls, word, pos_tag):
        tag = cls._get_wordnet_pos(pos_tag)
        if tag:
            word = cls.lmtzr.lemmatize(word, tag)
        return word

    @staticmethod
    def _get_wordnet_pos(treebank_tag):
        if treebank_tag.startswith('J'):
            return wordnet.ADJ
        elif treebank_tag.startswith('V'):
            return wordnet.VERB
        elif treebank_tag.startswith('N'):
            return wordnet.NOUN
        elif treebank_tag.startswith('R'):
            return wordnet.ADV
        else:
            return None