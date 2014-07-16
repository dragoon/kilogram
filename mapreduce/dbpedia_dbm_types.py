"""
Prepares DBPedia type dict with entity URIs as keys and types as values.
We use pickle here since the dict is quite small and we need a set as value.
It then shipped with the job.
"""
from collections import defaultdict
import pickle
import subprocess

TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'<Agent>', '<TimePeriod>', 'PersonFunction'}

dbpediadb = defaultdict(list)
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + TYPES_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri, predicate, type_uri = line.split(' ', 2)
    except:
        continue
    if 'http://dbpedia.org/ontology/' not in type_uri:
        continue
    uri = uri.replace('http://dbpedia.org/resource/', '')
    type_uri = type_uri.replace('http://dbpedia.org/ontology/', '')[:-3]
    if type_uri in EXCLUDES:
        continue
    dbpediadb[uri].append(type_uri)

pickle.dump(dbpediadb, open('dbpedia_types.dbm', 'w'))
