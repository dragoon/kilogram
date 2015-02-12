from .ngram import *
from .ngram_service import *
from .lang.edit_parser import extract_edits, extract_filtered

DEBUG = True
# Define host and port for Stanford POS tagger service
ST_HOSTNAME = 'localhost'
ST_PORT = 2020
NER_HOSTNAME = 'localhost'
NER_PORT = 9191
