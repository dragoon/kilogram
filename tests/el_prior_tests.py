import unittest

from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.dataset.entity_linking.msnbc import DataSet
from kilogram.dataset.entity_linking.microposts import DataSet as Tweets
from kilogram.entity_linking import syntactic_subsumption, closeness_pruning
from kilogram.entity_linking.evaluation import Metrics
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService
import kilogram

kilogram.DEBUG = False

NgramService.configure(hbase_host=('diufpc304', '9090'))
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")

ngram_predictor = NgramTypePredictor('typogram')


class TestEntityLinking(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(TestEntityLinking, self).__init__(methodName)
        ace2004_data = DataSet('../extra/data/ace2004/texts/',
                                  '../extra/data/ace2004/ace2004.txt', ner)
        aquaint_data = DataSet('../extra/data/aquaint/texts/',
                                  '../extra/data/aquaint/aquaint.txt', ner)
        msnbc_data = DataSet('../extra/data/msnbc/texts/',
                                  '../extra/data/msnbc/msnbc_truth.txt', ner)
        self.datas = [ace2004_data, aquaint_data, msnbc_data]

    def test_d2kb(self):
        print('Prior prob, D2KB')
        for data_col in self.datas:
            print data_col.data_dir
            metric = Metrics()
            for datafile in data_col.data:
                syntactic_subsumption(datafile.candidates)
                for candidate in datafile:
                    # D2KB condition
                    if candidate.truth_data['uri'] is None:
                        continue
                    metric.evaluate(candidate.truth_data, candidate.get_max_uri())
            metric.print_metrics()
            print()

    def test_d2kb_typed(self):
        print('Prior prob + type improvements, D2KB')
        for data_col in self.datas:
            metric = Metrics()
            for datafile in data_col.data:
                syntactic_subsumption(datafile.candidates)
                for candidate in datafile:
                    candidate.init_context_types(ngram_predictor)
                for candidate in datafile:
                    # D2KB
                    if candidate.truth_data['uri'] is None:
                        continue
                    context_types_list = [c.context_types for c in datafile if c.context_types and c.cand_string == candidate.cand_string]
                    if context_types_list:
                        uri = candidate.get_max_typed_uri(context_types_list)
                    else:
                        uri = candidate.get_max_uri()

                    metric.evaluate(candidate.truth_data, uri)
            metric.print_metrics()
            print()


class TestEntityLinkingKBMicroposts(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(TestEntityLinkingKBMicroposts, self).__init__(methodName)
        self.microposts_data = Tweets('../extra/data/microposts2016/test_microposts.tsv', ner,
                                      handles_file='../extra/data/microposts2016/users.tsv')

    def test_a2kb(self):
        import pickle
        print('REL-RW, A2KB')
        metric = Metrics()
        import codecs
        out = codecs.open('results.tsv', 'w', 'utf-8')
        try:
            pickle_dict = pickle.load(open('related_uris.pcl'))
        except:
            pickle_dict = {}
        for datafile in self.microposts_data.data:
            syntactic_subsumption(datafile.candidates)
            closeness_pruning(datafile.candidates, pickle_dict)
            #for candidate in datafile:
            #    candidate.init_context_types(ngram_predictor)
            for candidate in datafile:
                if candidate.truth_data['uri'] and candidate.truth_data['uri'].startswith('NIL'):
                    continue
                uri = None
                if candidate.context is not None:
                    if not uri:
                        uri = candidate.get_max_uri()
                    if uri != candidate.truth_data['uri']:
                        print(uri, candidate.truth_data, candidate.cand_string)
                if uri:
                    uri = uri.decode('utf-8')
                else:
                    uri = unicode(uri)
                metric.evaluate(candidate.truth_data, uri)
                out.write(u'\t'.join([datafile.filename, candidate.cand_string,
                                     unicode(candidate.start_i), unicode(candidate.end_i), uri, datafile.text]) + u'\n')

        metric.print_metrics()
        out.close()
        pickle.dump(pickle_dict, open('related_uris.pcl', 'w'))


if __name__ == '__main__':
    print('Test PriorProb Entity Linking')
    unittest.main()
