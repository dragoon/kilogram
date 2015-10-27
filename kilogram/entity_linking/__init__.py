from kilogram import ListPacker


def parse_candidate(cand_string):
    candidates = ListPacker.unpack(cand_string)
    return [(uri, long(count)) for uri, count in candidates]
