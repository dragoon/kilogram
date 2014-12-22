from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures
from kilogram.lang.wordnet import WordNetNgram
from .ngram_service import NgramService, SUBSTITUTION_TOKEN


class Ngram(object):

    @staticmethod
    def ngram_freq(ngrams):
        """
        :param ngrams: list of n-gram to query counts
        :returns: list of counts, size=1 if not a substitution,
        otherwise available counts for all substitutions
        :rtype: list
        """
        result = {}
        for ngram in ngrams:
            result.update(NgramService.get_freq(ngram))
        return FreqDist(result)


class EditNgram(Ngram):
    def __init__(self, ngram, edit_pos):
        """
        :param edit_pos: position of substitution in an n-gram
        :type ngram: list
        :type edit_pos: int
        """
        assert hasattr(ngram, '__iter__')
        self.ngram = ngram
        self.pos_tag = None
        self.edit_pos = edit_pos
        self._association_dict = {}
        self._ngram_size = len(ngram)

    def __unicode__(self):
        return u' '.join(self.ngram)

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def normal_position(self):
        new_pos = 0
        if self.edit_pos == (self._ngram_size - 1):
            new_pos = -1
        elif self.edit_pos == 0:
            new_pos = 1
        return new_pos

    @property
    def subst_ngram(self):
        ngram = list(self.ngram)
        ngram[self.edit_pos] = SUBSTITUTION_TOKEN
        return ngram

    def association(self, measure='pmi'):
        if measure in self._association_dict:
                return self._association_dict[measure]

        ngrams = [self.ngram]
        collocs = {}

        for ngram in ngrams:
            self.ngram = ngram
            dist = self._get_freq_distributions()

            if len(self.ngram) == 2:
                finder = BigramCollocationFinder(*dist)
                measures = getattr(bigram_measures, measure)
            else:
                finder = TrigramCollocationFinder(*dist)
                measures = getattr(trigram_measures, measure)

            try:
                collocs = finder.score_ngrams(measures)
                collocs = dict((x[0][self.edit_pos], (i, x[1])) for i, x in enumerate(collocs))
            except Exception, e:
                print 'Exception in pmi_preps'
                print e
                print self
                print dist
                collocs = {}
            self._association_dict[measure] = collocs
            if collocs:
                return collocs
        return collocs

    def _get_freq_distributions(self):
        subst_ngram = self.subst_ngram
        word_fd = self.ngram_freq(subst_ngram)
        whole_fd = self.ngram_freq([' '.join(subst_ngram)])
        n = len(self.ngram)
        if n == 2:
            dist = (word_fd, whole_fd)
        elif 3 <= n <= 4:
            # need to add wild-card and bigram distributions
            wildfd = self.ngram_freq([subst_ngram[0] + u' ' + u' '.join(subst_ngram[2:])])
            bfd = self.ngram_freq([u' '.join(subst_ngram[:2]), u' '.join(subst_ngram[1:])])
            dist = (word_fd, bfd, wildfd, whole_fd)
        else:
            raise NotImplementedError
        return dist


    def get_single_feature(self, correction):
        """
        Returns a feature vector for a particular n-gram. Used to predict n-gram importance.
        """
        collocs = self.association()
        pos_tag_feature = [int(x in self.pos_tag) for x in ('NN', 'VB', 'JJ', 'DT', 'RB')]
        feature = [self.normal_position, self._ngram_size, len(collocs)]
        feature.extend(pos_tag_feature)
        return feature, collocs.get(correction, 50)

