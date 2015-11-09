import unittest
from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import DataSet

from entity_linking.babelfy import _extract_candidates, link, CandidateEntity, SemanticGraph
from entity_linking.evaluation import Metrics
from kilogram import NgramService
import kilogram

NgramService.configure(hbase_host=('diufpc304', '9090'))
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
msnbc_data = DataSet('../extra/data/msnbc/texts/',
                        '../extra/data/msnbc/msnbc_truth.txt', ner)


class TestEntityLinking(unittest.TestCase):

    def test_extract_candidates(self):
        self.assertIsNotNone(_extract_candidates([("Obama", "NNP")]))
        self.assertEquals(len(_extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])), 2)

    def test_twitter(self):
        for line in open('/Users/dragoon/Downloads/en_tweets.txt'):
            text = line.strip().split('\t')[1]
            print link(text)

    def test_entity_linking(self):
        print link("After his departure from Buffalo, Saban returned to coach college football teams including Miami, Army and UCF.")
        print link("Barack and Michelle visited us today.")
        print link("GitHub experienced a massive DDoS attack yesterday evening.")
        print link("Saban, previously a head coach of NFL's Miami, is now coaching Crimson Tide. "
                   "His achievements include leading LSU to the BCS National Championship once and Alabama three times.")

    def test_prior_prob_a2kb(self):
        print 'Prior prob, A2KB'
        metric = Metrics()
        for filename, values in msnbc_data.data.iteritems():
            candidates = []
            for i, line_dict in enumerate(values):
                text = line_dict['text']
                # i ensures different nouns
                cand = CandidateEntity(0, 0, i, text)
                if cand.uri_counts:
                    line_dict['cand'] = cand
                    candidates.append(cand)
            # resolve
            graph = SemanticGraph(candidates)
            graph.do_iterative_removal()
            graph.do_linking()
            for line_dict in values:
                true_uri = line_dict['true_uri']
                uri = None
                if 'cand' in line_dict:
                    uri = line_dict['cand'].true_entity
                metric.evaluate(true_uri, uri)
        metric.print_metrics()

    def test_prior_prob_d2kb(self):
        print 'Prior prob, D2KB'
        metric = Metrics()
        for filename, values in msnbc_data.data.iteritems():
            candidates = []
            for i, line_dict in enumerate(values):
                if line_dict['true_uri']['uri'] is None:
                    continue
                text = line_dict['text']
                cand = CandidateEntity(0, 0, i, text)
                if cand.uri_counts:
                    line_dict['cand'] = cand
                    candidates.append(cand)
            # resolve
            graph = SemanticGraph(candidates)
            graph.do_iterative_removal()
            graph.do_linking()
            for line_dict in values:
                if line_dict['true_uri']['uri'] is None:
                    continue
                true_uri = line_dict['true_uri']
                uri = None
                if 'cand' in line_dict:
                    uri = line_dict['cand'].true_entity
                metric.evaluate(true_uri, uri)
        metric.print_metrics()


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
