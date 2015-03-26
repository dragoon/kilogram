import rdflib

__author__ = 'Roman Prokofyev'


class DBPediaOntology:
    dbpedia_types = None

    def __init__(self, owl_filename):
        self.dbpedia_types = {}
        g = rdflib.Graph()
        g.parse(owl_filename, format="xml")
        for subject, predicate, obj in g:
            if str(predicate) == 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
                obj = str(obj)
                subject = str(subject)
                if 'http://dbpedia.org/ontology/' in obj \
                        and 'http://dbpedia.org/ontology/' in subject:
                    obj = obj.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                    subject = subject.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                    self.dbpedia_types[subject] = obj

    def get_parent(self, entity_type, dest_parent=None):
        try:
            while self.dbpedia_types.get(entity_type, None) != dest_parent:
                entity_type = self.dbpedia_types[entity_type]
            return entity_type
        except KeyError:
            return None
