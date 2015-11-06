from __future__ import division


class Metrics(object):
    fp = None
    tp = None
    fn = None
    tn = None

    def __init__(self):
        self.tp = 0
        self.fp = 0
        self.fn = 0
        self.tn = 0

    def evaluate(self, true_uri, uri):
        if uri == true_uri['uri']:
            self.tp += 1
        elif uri is None and true_uri['uri'] is not None:
            if true_uri['exists']:
                self.fn += 1
        else:
            self.fp += 1

    def print_metrics(self):
        precision = self.tp / (self.tp+self.fp)
        recall = self.tp / (self.tp+self.fn)
        print 'P =', precision, 'R =', recall, 'F1 =', 2*precision*recall/(precision+recall)
        print self.tp, self.fp, self.fn, self.tn