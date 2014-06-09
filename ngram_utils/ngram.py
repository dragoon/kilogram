from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures
from . import ngram_service

class Ngram(object):
    SUBSTITUTION = 'SUB'
    
    @staticmethod
    def _is_subst(ngram):
        return Ngram.SUBSTITUTION in set(ngram.split())
    
    @staticmethod
    def ngram_freq(ngram):
        """
        :returns: list of counts, size=1 if not a substitution, otherwise available counts for all substitutions
        :rtype: list
        """
        return ngram_service.NgramService.get_freq(ngram, Ngram._is_subst(ngram))


class EditNgram(Ngram):
    def __init__(self, ngram, edit_pos):
        """:type ngram: list"""
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
        subst_ngram = self.ngram[:]
        subst_ngram[self.edit_pos] = self.SUBSTITUTION
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
        if n == 1:
            dist = (word_fd, whole_fd)
        elif n == 2:
            # need to add wild-card and bigram distributions
            wildfd = self.ngram_freq([subst_ngram[0] + u' ' + subst_ngram[2]])
            bfd = self.ngram_freq([subst_ngram[0] + u' ' + subst_ngram[1], subst_ngram[1] + u' ' + subst_ngram[2]])
            dist = (word_fd, bfd, wildfd, whole_fd)
        else:
            raise NotImplementedError
        return dist
