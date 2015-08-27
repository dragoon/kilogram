import rdflib
import nltk
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
                dbpedia_types_reverse[obj].append(subject)
                if 'http://dbpedia.org/ontology/' in obj \
                        and 'http://dbpedia.org/ontology/' in subject:
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


class NgramEntityResolver:
    dbpedia_types = None
    uri_excludes = None
    lower_includes = None

    def __init__(self, types_file, uri_excludes, lower_uri_includes, owl_filename):
        self.dbpedia_types = defaultdict(list)
        self.ontology = DBPediaOntology(owl_filename)

        for line in open(types_file):
            entity, entity_type = line.strip().split('\t')
            self.dbpedia_types[entity].append(entity_type)

        self.uri_excludes = set(open(uri_excludes).read().splitlines())
        self.lower_includes = dict([line.strip().split('\t') for line in open(lower_uri_includes)])

    def resolve_entities(self, words):
        """Recursive entity resolution"""
        for i in range(len(words), 0, -1):
            for j, ngram in enumerate(nltk.ngrams(words, i)):
                ngram_joined = ' '.join(ngram)
                label = ngram_joined.replace(' ', '_')
                if label in self.lower_includes:
                    label = self.lower_includes[label]
                if label not in self.uri_excludes and label in self.dbpedia_types:
                    # check canonical uri
                    uri = '<dbpedia:'+label+'>'
                    new_words = []
                    new_words.extend(self.resolve_entities(words[:j]))
                    new_words.append(uri)
                    new_words.extend(self.resolve_entities(words[j+len(ngram):]))
                    return new_words
        return words

    def replace_types(self, words, order=0):
        for word in words:
            if word.startswith('<dbpedia:'):
                types = self.dbpedia_types[word[9:-1]]
                yield '<dbpedia:' + self.ontology.get_ordered_types(types)[order] + '>'
            else:
                yield word
