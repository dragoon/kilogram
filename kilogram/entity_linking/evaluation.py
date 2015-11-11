from __future__ import division


class Metrics(object):
    fp = None
    tp = None
    fn = None
    tn = None
    save_file = None

    def __init__(self, save_file=None):
        self.tp = 0
        self.fp = 0
        self.fn = 0
        self.tn = 0
        if save_file:
            self.save_file = open(save_file, 'w')

    def evaluate(self, true_uri, uri):
        if self.save_file:
            self.save_file.write(str(true_uri['uri']) + ', ' + str(uri) + '\n')
        if uri == true_uri['uri']:
            if uri is None:
                self.tn += 1
            else:
                self.tp += 1
        elif uri is None and true_uri['uri'] is not None:
            if true_uri['exists']:
                self.fn += 1
        else:
            self.fp += 1

    def print_metrics(self):
        if self.save_file:
            self.save_file.close()
        precision = self.tp / (self.tp+self.fp)
        recall = self.tp / (self.tp+self.fn)
        print 'P =', precision, 'R =', recall, 'F1 =', 2*precision*recall/(precision+recall)
        print self.tp, self.fp, self.fn, self.tn
