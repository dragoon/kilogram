"""
Prepares DBPedia type dict with entity URIs as keys and types as values.
We use shelve here since the dict is quite large in memory(~2G) and we need a set as value.
It then shipped with the job.
"""
import shelve
import subprocess

TYPES_FILE = 'instance_types_en.nt.bz2'
EXCLUDES = {'<Agent>', '<TimePeriod>', '<PersonFunction>'}

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
    uri = uri.replace('http://dbpedia.org/resource/', '')
    type_uri = type_uri.replace('http://dbpedia.org/ontology/', '')[:-3]
    if type_uri in EXCLUDES:
        continue
    if uri not in dbpediadb:
        dbpediadb[uri] = [type_uri]
    else:
        dbpediadb[uri].append(type_uri)

dbpediadb.close()
