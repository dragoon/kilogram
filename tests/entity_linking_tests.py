import unittest
from kilogram.entity_linking import _extract_candidates, link
from kilogram import NgramService


class TestEntityLinking(unittest.TestCase):
    def setUp(self):
        NgramService.configure(hbase_host=('diufpc304', '9090'))

    def test_priority_queue(self):
        import heapq
        candidates = _extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])
        heapq.heapify(candidates)
        item = heapq.heappop(candidates)
        self.assertEquals(str(item), "walk")

    def test_extract_candidates(self):
        self.assertIsNotNone(_extract_candidates([("Obama", "NNP")]))
        self.assertEquals(len(_extract_candidates([('Obama', 'NNP'), ('went', 'VBD'), ('with', 'IN'), ('me', 'PRP'), ('for', 'IN'), ('a', 'DT'), ('walk', 'NN'), ('.', '.')])), 2)

    def test_entity_linking(self):
        print link("Barack Obama and Michelle visited us today.")
        print link("GitHub experienced a massive DDoS attack yesterday evening.")


if __name__ == '__main__':
    print('Test Entity Linkings')
    unittest.main()
