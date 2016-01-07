import numpy as np
from gensim.matutils import unitvec
from gensim.models import word2vec
# Import the built-in logging module and configure it so that Word2Vec
# creates nice output messages
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def train_model(source_file, num_features=128, min_word_count=40, num_workers=10, context=10, downsampling=1e-3):
    # LOW DIMENSIONALITY FOR TYPE SIMILARITY
    text = word2vec.LineSentence(source_file)
    model = word2vec.Word2Vec(text, workers=num_workers, size=num_features,
                              min_count=min_word_count, window=context, sample=downsampling, sg=0)
    model.init_sims(replace=True)
    model.save(str(num_features)+"features"+str(min_word_count)+"minwords"+str(context)+"context"+"_"+source_file)
    return model


class TypePredictionModel(object):
    clf = None
    word2vec_model = None
    available_types = None

    def __init__(self, word2vec_model, type_train_file=None, type_hierarchy=None):
        """
        :type word2vec_model: word2vec.Word2Vec
        """
        self.type_hierarchy = type_hierarchy
        from sklearn import linear_model
        self.word2vec_model = word2vec.Word2Vec.load(word2vec_model)
        self.word2vec_model.init_sims()

        self.available_types = [x for x in self.word2vec_model.vocab.keys() if x.startswith('<dbpedia:')]
        self.available_types.append('thing')
        self.clf = linear_model.LinearRegression()
        if type_train_file:
            self.fit(type_train_file)

    def fit(self, type_train_file):
        model = self.word2vec_model
        X = []
        y = []
        for line in open(type_train_file):
            ngram, entity_type = line.strip().split('\t')
            ngram_vec = model[ngram]
            entity_type_vec = model[entity_type]
            X.append(ngram_vec)
            y.append(entity_type_vec)

        self.clf.fit(X, y)

    def _wordvec_similarity(self, word, vector):
        return np.dot(unitvec(self.word2vec_model[word]), vector)

    def predict_types_similarity(self, ngram):
        types_ranked = []
        for entity_type in self.available_types:
            try:
                score = self.word2vec_model.similarity(ngram, entity_type)
            except KeyError:
                break
            generic_type = self.type_hierarchy.get_parent(entity_type, '<dbpedia:Agent>')
            if not generic_type:
                generic_type = self.type_hierarchy.get_parent(entity_type, None)
            types_ranked.append((generic_type, score))
        return [sorted(types_ranked, key=lambda x: x[1], reverse=True)]

    def predict_types_linear(self, ngram):
        types_ranked = []
        predicted_vector = self.clf.predict(self.word2vec_model[ngram])
        for entity_type in self.available_types:
            try:
                types_ranked.append((entity_type, self._wordvec_similarity(entity_type, predicted_vector)))
            except KeyError:
                continue

        return [sorted(types_ranked, key=lambda x: x[1], reverse=True)]

    def similarity(self, words):
        scores = []
        for entity_type in self.available_types:
            score = 0
            for word in words:
                try:
                    score += self.word2vec_model.similarity(word, entity_type)
                except KeyError:
                    break
            scores.append((entity_type, score))
        return sorted(scores, key=lambda x: x[1])


class NumberAnnotator(object):
    """Simple format: one sentence = one line; words already preprocessed and separated by whitespace."""
    def __init__(self, source):
        """
        :type source: iterable
        """
        self.source = source

    def __iter__(self):
        """Iterate through the lines in the source."""
        for line in self.source:
            yield line
