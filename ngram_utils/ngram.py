
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
        pass
        

class Ngram(object):
    pass
