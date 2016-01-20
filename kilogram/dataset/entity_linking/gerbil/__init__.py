from ....entity_linking import CandidateEntity
from ....lang import get_context, parse_entities


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
        ner_list = parse_entities(self.text)
        type_dict = dict([(e['text'], e['type']) for e in ner_list])
        for mention in self.mentions:
            context = get_context(mention['start'], mention['end'], self.text)
            candidate = CandidateEntity(0, 0, mention['name'], e_type=type_dict.get(mention['name']),
                                        context=context, ner=self.ner)
            candidates.append(candidate)
        return candidates
