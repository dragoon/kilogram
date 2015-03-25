from collections import defaultdict
import rdflib

__author__ = 'Roman Prokofyev'


def get_dbpedia_type_hierarchy(owl_filename):
    dbpedia_types = defaultdict(list)
    g = rdflib.Graph()
    g.parse(owl_filename, format="xml")
    for subject, predicate, obj in g:
        if str(predicate) == 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
            obj = str(obj)
            subject = str(subject)
            if 'http://dbpedia.org/ontology/' in obj and 'http://dbpedia.org/ontology/' in subject:
                obj = obj.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                subject = subject.replace('http://dbpedia.org/ontology/', '<dbpedia:') + '>'
                dbpedia_types[obj].append(subject)
    return dbpedia_types
