from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from libs.hbase import Hbase

class NgramService(object):

    def __init__(self, mongo_host, hbase_host):
    mclient = settings.MONGO_CLIENT
    unigram_db = mclient['unigrams']
    bigram_db = mclient['bigrams']
    trigram_db = mclient['trigrams']
    unigram_col_all = unigram_db['all']
    bigram_col_preps = bigram_db['preps']
    trigram_col_preps = trigram_db['preps']
    # No Determinatives
    trigram_db_nodt = mclient['tetragrams']
    bigram_db_nodt = mclient['bigrams_nodt']
    trigram_preps_nodt1 = trigram_db_nodt['preps1']
    trigram_preps_nodt2 = trigram_db_nodt['preps2']
    bigram_col_preps_nodt = bigram_db_nodt['preps']

    # HBASE
    h_unigrams = 'ngrams1'
    h_bigrams = 'ngrams2'
    h_trigrams_skips = 'ngrams3'
    transport = TTransport.TBufferedTransport(TSocket.TSocket(*settings.HBASE_HOST))
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Hbase.Client(protocol)
    transport.open()
    rate = 0
    start = time.time()
