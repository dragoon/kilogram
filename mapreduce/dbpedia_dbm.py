"""
Creates DBPedia labels-types file of the following format:

{ LABEL: [Type1, Type2, ...], ...}

For example:

Tramore:             Town, Settlement, PopulatedPlace, Place
Tramore,_Ireland:    Town, Settlement, PopulatedPlace, Place
"""
import codecs

import subprocess
import urllib
from collections import defaultdict


TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'Agent', 'TimePeriod', 'PersonFunction', 'Year'}

typed_entities = defaultdict(lambda: {'types': [], 'redirects': []})
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + TYPES_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    if '<BAD URI: Illegal character' in line:
        continue
    try:
        uri, predicate, type_uri = line.split(' ', 2)
    except:
        continue
    if '<http://schema.org/Person>' in type_uri:
        type_uri = '<http://dbpedia.org/ontology/Person>   '
    if 'http://dbpedia.org/ontology/' not in type_uri:
        continue
    if 'http://dbpedia.org/ontology/Wikidata' in type_uri:
        continue
    if 'http://dbpedia.org/ontology/Location' in type_uri:
        continue
    uri = urllib.unquote(uri.replace('<http://dbpedia.org/resource/', '')[:-1])
    type_uri = type_uri.replace('<http://dbpedia.org/ontology/', '')[:-4]
    if type_uri in EXCLUDES:
        continue

    uri = uri.decode('utf-8')
    typed_entities[uri]['types'].append(type_uri)


REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + REDIRECTS_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri_redirect, predicate, uri_canon = line.split(' ', 2)
    except:
        continue
    name_redirect = urllib.unquote(uri_redirect.replace('<http://dbpedia.org/resource/', '')[:-1])
    name_canon = urllib.unquote(uri_canon.replace('<http://dbpedia.org/resource/', '')[:-4])
    if '(disambiguation)' in name_redirect:
        continue

    typed_entities[name_canon.decode('utf-8')]['redirects'].append(name_redirect.decode('utf-8'))

dbpedia_data = codecs.open('dbpedia_data.txt', 'w', 'utf-8')

for uri, value_dict in typed_entities.iteritems():
    dbpedia_data.write(uri+'\t'+' '.join(value_dict['types'])+'\t'+' '.join(value_dict['redirects'])+'\n')

dbpedia_data.close()
