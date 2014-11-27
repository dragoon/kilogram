from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures
from thrift.Thrift import TApplicationException
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
            try:
                result.update(NgramService.get_freq(ngram))
            except TApplicationException:
                print 'EXCEPTION NGRAM', ngram
                print
                raise
        return FreqDist(result)


class EditNgram(Ngram):
    def __init__(self, ngram, edit_pos):
        """
        :param edit_pos: position of substitution in an n-gram
        :type ngram: list
        :type edit_pos: int
        :type pos_tag: list
        """
        assert hasattr(ngram, '__iter__')
        self.ngram = ngram
        self.pos_tag = None
        self.edit_pos = edit_pos
        self._association_dict = {}

    def __unicode__(self):
        return u' '.join(self.ngram)

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def subst_ngram(self):
        ngram = list(self.ngram)
        ngram[self.edit_pos] = SUBSTITUTION_TOKEN
        return ngram

    def association(self, measure='pmi'):
        if measure in self._association_dict:
            return self._association_dict[measure]
        dist = self._get_freq_distributions()

        if len(self.ngram) == 2:
            finder = BigramCollocationFinder(*dist)
            measures = getattr(bigram_measures, measure)
        else:
            finder = TrigramCollocationFinder(*dist)
            measures = getattr(trigram_measures, measure)

        try:
            collocs = finder.score_ngrams(measures)
        except Exception, e:
            print 'Exception in pmi_preps'
            print e
            print self
            print dist
            collocs = []
        self._association_dict[measure] = collocs
        return collocs

    def _get_freq_distributions(self):
        subst_ngram = self.subst_ngram
        word_fd = self.ngram_freq(subst_ngram)
        whole_fd = self.ngram_freq([' '.join(subst_ngram)])
        n = len(self.ngram)
        if n == 2:
            dist = (word_fd, whole_fd)
        elif n == 3:
            # need to add wild-card and bigram distributions
            wildfd = self.ngram_freq([subst_ngram[0] + u' ' + subst_ngram[2]])
            bfd = self.ngram_freq([subst_ngram[0] + u' ' + subst_ngram[1],
                                   subst_ngram[1] + u' ' + subst_ngram[2]])
            dist = (word_fd, bfd, wildfd, whole_fd)
        else:
            raise NotImplementedError
        return dist
