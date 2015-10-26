import unittest

from dataset.dbpedia import NgramEntityResolver
from dataset.msnbc import parse_data
from entity_linking.babelfy import _extract_candidates, link
from kilogram import NgramService


class TestEntityLinking(unittest.TestCase):
    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', '9090'))

    def test_extract_candidates(self):
        self.assertIsNotNone(_extract_candidates([("Obama", "NNP")]))
        self.assertEquals(len(_extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])), 2)

    def test_entity_linking(self):
        print link("After his departure from Buffalo, Saban returned to coach college football teams including Miami, Army and UCF.")
        print link("Barack and Michelle visited us today.")
        print link("GitHub experienced a massive DDoS attack yesterday evening.")
        print link("Saban, previously a head coach of NFL's Miami, is now coaching Crimson Tide. "
                   "His achievements include leading LSU to the BCS National Championship once and Alabama three times.")

    def test_prior_prob(self):
        ner = NgramEntityResolver("/home/roman/dbpedia/dbpedia_types.txt", "/home/roman/dbpedia/dbpedia_uri_excludes.txt", "/home/roman/dbpedia/dbpedia_lower_includes.txt", "/home/roman/dbpedia/dbpedia_redirects.txt", "/home/roman/dbpedia/dbpedia_2015-04.owl")
        data = parse_data('/home/roman/language_models/wiki/msnbc/msnbc_plain/', ner)


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
