"""
Creates DBPedia labels-types TSV file of the following format:

LABEL   CANONICAL_LABEL     Type1;Type2[;...]

For example:

Tramore             Tramore     Town;Settlement;PopulatedPlace;Place
Tramore,_Ireland    Tramore     Town;Settlement;PopulatedPlace;Place
"""

import subprocess
from collections import defaultdict

TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'Agent', 'TimePeriod', 'PersonFunction', 'Year'}

dbpediadb_types = defaultdict(list)
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

    dbpediadb_types[uri].append(type_uri)

dbpedia_types_tsv = open('dbpedia_types.tsv', 'w')

# write types first
for uri, types in dbpediadb_types.items():
    dbpedia_types_tsv.write('\t'.join([uri, uri, ';'.join(types)])+'\n')


REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
dbpediadb_labels = {}
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
    # skip entities that have no types
    if name_canon not in dbpediadb_types:
        continue
    dbpedia_types_tsv.write('\t'.join([name_redirect, name_canon, ';'.join(dbpediadb_types[name_canon])])+'\n')
dbpedia_types_tsv.close()
