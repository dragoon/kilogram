"""
Prepare DBPediaDB, we use DBM since in-memory python dictionary will occupy ~2GB and slow down the job.
We then ship the DB with the job
"""
import anydbm
import subprocess
from nltk.corpus import stopwords, words

STOPWORDS = set(stopwords.words('english') + words.words('en-basic'))
REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
EXCLUDES = set()

dbpediadb = anydbm.open('dbpedia.dbm', 'c')
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + REDIRECTS_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri_redirect, predicate, uri_canon = line.split(' ', 2)
    except:
        continue
    name_redirect = uri_redirect.replace('<http://dbpedia.org/resource/', '')[:-1]
    name_canon = uri_canon.replace('<http://dbpedia.org/resource/', '')[:-4].replace('_', ' ')
    if len(name_canon) < 2:
        continue
    if name_canon.lower() in STOPWORDS:
        continue
    if '(disambiguation)' in name_redirect:
        EXCLUDES.add(name_canon)
    dbpediadb[name_canon] = '<' + name_canon.replace(' ', '_') + '>'

for name in EXCLUDES:
    del dbpediadb[name]

dbpediadb.close()
