"""
Creates DBPedia set with entity **MAIN** labels as entries.
It then shipped with the appropriate mapreduce job.

Format: {'Historical Vedic religion': '<Historical_Vedic_religion>', ...}
"""
import subprocess
from nltk.corpus import stopwords, words

STOPWORDS = set(stopwords.words('english') + words.words('en-basic'))
REDIRECTS_FILE = 'redirects_transitive_en.nt.bz2'
EXCLUDES = set()

dbpedia_set = set()
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
    dbpedia_set.add(name_canon)

# remove in the end because disambiguations might have been added after
for name in EXCLUDES:
    dbpedia_set.discard(name)

# save
open('dbpedia_labels.txt').write('\n'.join(dbpedia_set))
