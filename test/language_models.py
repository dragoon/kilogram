from nltk import ConditionalFreqDist, ConditionalProbDist, SimpleGoodTuringProbDist
from nltk.model import NgramModel


def _estimator(fdist, bins):
    """
    Default estimator function using a SimpleGoodTuringProbDist.
    """
    # can't be an instance method of NgramModel as they
    # can't be pickled either.
    return SimpleGoodTuringProbDist(fdist)


class PreparedNgramModel(NgramModel):
    """
    Build a model from Google N-gram.
    NLTK's implementation consumes insane amount of RAM, for 3G file (1+2grams) you would need ~50G.
    """

    def __init__(self, n, ngram_file, pad_left=True, pad_right=False,
                 estimator=None, *estimator_args, **estimator_kwargs):

        self._n = n
        self._lpad = ('',) * (n - 1) if pad_left else ()
        self._rpad = ('',) * (n - 1) if pad_right else ()

        if estimator is None:
            estimator = _estimator

        cfd = ConditionalFreqDist()
        self._ngrams = set()

        for line in open(ngram_file):
            ngram, count = line.strip().split('\t')
            ngram = tuple(ngram.split())
            if len(ngram) == n:
                self._ngrams.add(ngram)
                context = tuple(ngram[:-1])
                token = ngram[-1]
                cfd[context].inc(token, long(count))

        if not estimator_args and not estimator_kwargs:
            self._model = ConditionalProbDist(cfd, estimator, len(cfd))
        else:
            self._model = ConditionalProbDist(cfd, estimator, *estimator_args, **estimator_kwargs)

        # recursively construct the lower-order models
        if n > 1:
            self._backoff = PreparedNgramModel(n-1, ngram_file, pad_left, pad_right,
                                       estimator, *estimator_args, **estimator_kwargs)