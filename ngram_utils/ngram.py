from nltk import FreqDist, BigramCollocationFinder, TrigramCollocationFinder
from nltk.collocations import TrigramAssocMeasures as trigram_measures
from nltk.collocations import BigramAssocMeasures as bigram_measures


class EditNgram(object):
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
    def pmi_preps():
        dist = self._get_freq_distributions()
        
        if len(ngram) == 2:
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
            print ngram_score
            print ngram_score.dist
            collocs = []
        return collocs
        

class Ngram(object):
    pass
