from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures


class EditNgram(object):
    SUBSTITUTION = 'SUB'
    def __init__(self, ngram, edit_pos):
        """:type ngram: list"""
        self.ngram = ngram
        self.edit_pos = edit_pos
        self.ngram[edit_pos] = self.SUBSTITUTION

    def __unicode__(self):
        return u' '.join(self.ngram)
        
    def __repr__(self):
        return unicode(self).encode('utf-8') 
    
    def __str__(self):
        return unicode(self).encode('utf-8')
        
    @property
    def pmi_preps():
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
        word_fd = self._prep_wfd(ngram)
        if n == 1:
            dist = (word_fd, self._prep_ngram_distribution(position, ngram))
        elif n == 2:
            # need to add wild card distribution and bigram distribution
            tfd = self._prep_ngram_distribution(position, ngram)
            wildfd = self._prep_wildfd(position, ngram)
            bfd = self._prep_bfd(position, ngram)
            dist = (word_fd, bfd, wildfd, tfd)
        else:
            raise NotImplementedError
        return dist
        

class Ngram(object):
    pass
