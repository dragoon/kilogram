import numpy as np
from gensim.matutils import unitvec
from gensim.models import word2vec
# Import the built-in logging module and configure it so that Word2Vec
# creates nice output messages
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def train_model(num_features=40, min_word_count=40, num_workers=10, context=10, downsampling=1e-3):
    # LOW DIMENSIONALITY FOR TYPE SIMILARITY
    text = word2vec.LineSentence('wiki_text_total_spotlight_specific.txt')
    model = word2vec.Word2Vec(text, workers=num_workers, size=num_features,
                              min_count=min_word_count, window=context, sample=downsampling, sg=0)
    model.init_sims(replace=True)
    model.save(str(num_features)+"features"+str(min_word_count)+"minwords"+str(context)+"context"+"_wiki_specific")
    return model


class TypePredictionModel(object):
    clf = None
    word2vec_model = None

    def __init__(self, word2vec_model):
        """
        :type word2vec_model: word2vec.Word2Vec
        """
        from sklearn import linear_model
        self.word2vec_model = word2vec_model
        self.clf = linear_model.LinearRegression()

    def fit(self, X, y):
        self.clf.fit(X, y)

    def get_types(self, ngram):
        return self.clf.predict(self.word2vec_model[ngram])

    def _most_similar_vect(self, vectenter, topn=10):
        vect_unit = unitvec(vectenter)
        dists = np.dot(self.word2vec_model.syn0norm, vect_unit)
        best = np.argsort(dists)[::-1][:topn]
        # ignore (don't return) words from the input
        result = [(self.word2vec_model.index2word[sim], float(dists[sim])) for sim in best]
        return result[:topn]



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