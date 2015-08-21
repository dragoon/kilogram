import rdflib
from collections import defaultdict

__author__ = 'Roman Prokofyev'


class DBPediaOntology:
    dbpedia_types = None
    dbpedia_types_order = None

    def __init__(self, owl_filename):
        self.dbpedia_types = {}
        dbpedia_types_reverse = defaultdict(list)
        g = rdflib.Graph()
        g.parse(owl_filename, format="xml")
        for subject, predicate, obj in g:
            if str(predicate) == 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
                obj = str(obj)
                subject = str(subject)
                if 'http://dbpedia.org/ontology/' in obj \
                        and 'http://dbpedia.org/ontology/' in subject:
                    dbpedia_types_reverse[obj].append(subject)
                    obj = obj.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                    subject = subject.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                    self.dbpedia_types[subject] = obj

        # put types in order
        #BFS traversal
        self.dbpedia_types_order = {}
        nodes = [('http://www.w3.org/2002/07/owl#Thing', 0)]
        while nodes:
            node, order = nodes.pop(0)
            self.dbpedia_types_order[node.replace('http://dbpedia.org/ontology/', '')] = order
            for child in dbpedia_types_reverse[node]:
                nodes.append((child, order+1))

    def get_parent(self, entity_type, dest_parent=None):
        try:
            while self.dbpedia_types.get(entity_type, None) != dest_parent:
                entity_type = self.dbpedia_types[entity_type]
            return entity_type
        except KeyError:
            return None

    def get_ordered_types(self, types):
        return sorted(types, key=lambda x: self.dbpedia_types_order[x], reverse=True)
