"""
Creates DBPedia dict with **ALL** entity labels as keys and canonical URIs as values.

Format: {'<Timeline_of_the_cosmos>': '<Chronology_of_the_universe>',
         '<Lockheed_C-5>': '<Lockheed_C-5_Galaxy>', ...}
"""
import anydbm
import subprocess

REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
EXCLUDES = set()

dbpediadb = anydbm.open('dbpedia_redirects.dbm', 'c')
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + REDIRECTS_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri_redirect, predicate, uri_canon = line.split(' ', 2)
    except:
        continue
    name_redirect = uri_redirect.replace('http://dbpedia.org/resource/', '')
    name_canon = uri_canon.replace('http://dbpedia.org/resource/', '')[:-3]
    if '(disambiguation)' in name_redirect:
        continue
    dbpediadb[name_redirect] = name_canon

dbpediadb.close()
