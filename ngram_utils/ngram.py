from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures
from .ngram_service import NgramService, SUBSTITUTION_TOKEN

class Ngram(object):
    
    @staticmethod
    def ngram_freq(ngrams):
        """
        :param ngrams: list of n-gram to query counts
        :returns: list of counts, size=1 if not a substitution, otherwise available counts for all substitutions
        :rtype: list
        """
        result = {}
        for ngram in ngrams:
            result.update(NgramService.get_freq(ngram))
        return FreqDist(result)


class EditNgram(Ngram):
    def __init__(self, ngram, edit_pos):
        """:type ngram: list"""
        assert hasattr(ngram, '__iter__')
        self.ngram = ngram
        self.edit_pos = edit_pos

    def __unicode__(self):
        return u' '.join(self.ngram)
        
    def __repr__(self):
        return unicode(self).encode('utf-8') 
    
    def __str__(self):
        return unicode(self).encode('utf-8')
        
    @property
    def subst_ngram(self):
        subst_ngram = list(self.ngram)
        subst_ngram[self.edit_pos] = SUBSTITUTION_TOKEN
        return subst_ngram
        
    @property
    def pmi_preps(self):
        dist = self._get_freq_distributions()
        
        if len(self.ngram) == 2:
            finder = BigramCollocationFinder(*dist)
            measures = bigram_measures.pmi
        else:
            finder = TrigramCollocationFinder(*dist)
            measures = trigram_measures.pmi

        try:
            collocs = finder.score_ngrams(measures)
        except Exception, e:
            print 'Exception in pmi_preps'
            print e
            print self
            print dist
            collocs = []
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
            bfd = self.ngram_freq([subst_ngram[0] + u' ' + subst_ngram[1], subst_ngram[1] + u' ' + subst_ngram[2]])
            dist = (word_fd, bfd, wildfd, whole_fd)
        else:
            raise NotImplementedError
        return dist
