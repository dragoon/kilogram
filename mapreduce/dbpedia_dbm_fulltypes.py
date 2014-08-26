"""
Creates DBPedia 2 dicts:
* one with an entity label as a key and an entity main uri as a value.
* second dict with **MAIN** entity labels as keys and types as values.
We use shelve here since the dict is quite large in memory(~2G) and we need a set as value.
It then shipped with the job.

Type edit format: {'Tramore': ['Town', 'Settlement', 'PopulatedPlace', 'Place'], ...}
"""

import shelve
import subprocess
from collections import defaultdict

TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'Agent', 'TimePeriod', 'PersonFunction', 'Year'}

dbpediadb = shelve.open('dbpedia_types.dbm', writeback=True)
dbpediadb_types = dbpediadb['types'] = {}
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + TYPES_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri, predicate, type_uri = line.split(' ', 2)
    except:
        continue
    if 'http://dbpedia.org/ontology/' not in type_uri:
        continue
    uri = uri.replace('<http://dbpedia.org/resource/', '')[:-1]
    type_uri = type_uri.replace('<http://dbpedia.org/ontology/', '')[:-4]
    if type_uri in EXCLUDES:
        continue

    if uri in dbpediadb_types:
        dbpediadb_types[uri].append(type_uri)
    else:
        dbpediadb_types[uri] = [type_uri]


REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
dbpediadb_labels = dbpediadb['labels'] = {}
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + REDIRECTS_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri_redirect, predicate, uri_canon = line.split(' ', 2)
    except:
        continue
    name_redirect = uri_redirect.replace('<http://dbpedia.org/resource/', '')[:-1]
    name_canon = uri_canon.replace('<http://dbpedia.org/resource/', '')[:-4]
    # do not build mapping for entities that have no types
    if name_canon not in dbpediadb_types:
        continue
    dbpediadb_labels[name_canon] = name_canon
    if '(disambiguation)' in name_redirect:
        continue
    dbpediadb_labels[name_redirect] = name_canon

dbpediadb.close()
