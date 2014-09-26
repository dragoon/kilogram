"""
Creates DBPedia labels-types Shelve file of the following format:

{ LABEL: [Type1, Type2, ...], ...}

For example:

Tramore:             Town, Settlement, PopulatedPlace, Place
Tramore,_Ireland:    Town, Settlement, PopulatedPlace, Place
"""

import subprocess
import urllib
from collections import defaultdict
import shelve

TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'Agent', 'TimePeriod', 'PersonFunction', 'Year'}

dbpediadb_types = defaultdict(list)
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + TYPES_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    if '<BAD URI: Illegal character' in line:
        continue
    try:
        uri, predicate, type_uri = line.split(' ', 2)
    except:
        continue
    if 'http://dbpedia.org/ontology/' not in type_uri:
        continue
    uri = urllib.unquote(uri.replace('<http://dbpedia.org/resource/', '')[:-1])
    type_uri = type_uri.replace('<http://dbpedia.org/ontology/', '')[:-4]
    if type_uri in EXCLUDES:
        continue

    dbpediadb_types[uri].append(type_uri)

dbpediadb = shelve.open('dbpedia_types.dbm')
dbpediadb_lower = shelve.open('dbpedia_lowercase2labels.dbm', writeback=True)

# write canonical labels first
for uri, types in dbpediadb_types.items():
    dbpediadb[uri] = types
    dbpediadb_lower[uri.lower()] = [uri]


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
    # skip entities that have no types
    if name_canon not in dbpediadb_types:
        continue
    dbpediadb[name_redirect] = dbpediadb_types[name_canon]
    if name_redirect.lower() in dbpediadb_lower:
        dbpediadb_lower[name_redirect.lower()].append(name_redirect)
    else:
        dbpediadb_lower[name_redirect.lower()] = [name_redirect]

dbpediadb.close()
