from ....entity_linking import CandidateEntity
from ....lang import get_context


class DataSet(object):
    text = None
    ner = None
    mentions = None
    data = None

    def __init__(self, text, mentions, ner):
        self.text = text
        self.ner = ner
        self.mentions = mentions
        self.candidates = self._parse_candidates()

    def _parse_candidates(self):
        candidates = []
        for mention in self.mentions:
            context = get_context(mention['start'], mention['end'], self.text)
            candidate = CandidateEntity(0, 0, mention['name'], e_type=None,
                                        context=context, ner=self.ner)
            candidates.append(candidate)
        return candidates
