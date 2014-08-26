"""
Creates DBPedia type dict with **ALL** entity labels as keys and types as values.
We use shelve here since the dict is quite large in memory(~2G) and we need a set as value.
It then shipped with the job.
This can be inefficient since the types list is duplicated on serialization, but given the size
we don't care much.

Format: {'Tramore': ['Town', 'Settlement', 'PopulatedPlace', 'Place'], ...}
"""

import shelve
import subprocess
from collections import defaultdict

REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
redirects = defaultdict(list)
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + REDIRECTS_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri_redirect, predicate, uri_canon = line.split(' ', 2)
    except:
        continue
    name_redirect = uri_redirect.replace('<http://dbpedia.org/resource/', '')[:-1]
    name_canon = uri_canon.replace('<http://dbpedia.org/resource/', '')[:-4]
    if '(disambiguation)' in name_redirect:
        continue
    redirects[name_canon].add(name_redirect)


TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'Agent', 'TimePeriod', 'PersonFunction', 'Year'}

dbpediadb = shelve.open('dbpedia_types.dbm', writeback=True)
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

    for alt_name in redirects[uri]:
        if alt_name not in dbpediadb:
            dbpediadb[alt_name] = [type_uri]
        else:
            dbpediadb[alt_name].append(type_uri)

dbpediadb.close()
