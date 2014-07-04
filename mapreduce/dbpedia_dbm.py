"""
Prepare DBPediaDB, we use DBM since in-memory python dictionary will occupy ~2GB and slow down the job.
We then ship the DB with the job
"""
import anydbm
import subprocess
DBPEDIA_FILE = 'labels_en.nt.bz2'

dbpediadb = anydbm.open('dbpedia.dbm', 'n')
# BZ2File module cannot process multi-stream files, so use subprocess
p = subprocess.Popen('bzcat -q ' + DBPEDIA_FILE, shell=True, stdout=subprocess.PIPE)
for line in p.stdout:
    try:
        uri, predicate, name = line.split(' ', 2)
    except:
        # continue for the first line
        continue
    name = name[1:-7]
    dbpediadb[name] = uri

dbpediadb.close()
