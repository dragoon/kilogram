import sys
import kilogram
from kilogram.dataset.dbpedia import NgramEntityResolver
from kilogram.dataset.entity_linking.microposts import DataFile, DataSet
from kilogram.entity_linking import CandidateEntity, Entity, syntactic_subsumption
from kilogram.lang import parse_tweet_entities, parse_entities_from_xml

print('Loading data...')
CandidateEntity.uri_counts_local = CandidateEntity.init_uri_counts_local()

ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")
kilogram.NER_HOSTNAME = 'localhost'

print('Starting annotations...')
import codecs
out = codecs.open('results.tsv', 'w', 'utf-8')
for line in sys.stdin:
    line = line.decode('utf-8')
    tweet_id, timestamp, user_id, user_name, user_description, tweet, tweet_ner = line.strip().split('\t')
    datafile = DataFile(tweet_id, tweet)

    tweet_ne_list = parse_tweet_entities(datafile.text)
    tweet_ne_names = set([x['text'] for x in tweet_ne_list])
    ner_list = parse_entities_from_xml(datafile.text, tweet_ner)
    ner_list = [x for x in ner_list if x['text'] not in tweet_ne_names] + tweet_ne_list

    for values in ner_list:
        cand_string = values['text']
        candidate = CandidateEntity(values['start_i'], values['end_i'], cand_string, e_type=values['type'],
                                    context=values['context'], ner=ner)
        astr_uri = DataSet._astrology_uri(candidate.cand_string)
        if astr_uri:
            candidate.entities = [Entity(astr_uri, 1, ner)]
        datafile.candidates.append(candidate)

    syntactic_subsumption(datafile.candidates)
    for candidate in datafile:
        uri = candidate.get_max_uri()
        if uri:
            uri = uri.decode('utf-8')
            out.write(u'\t'.join([tweet_id, candidate.cand_string, unicode(candidate.start_i),
                                  unicode(candidate.end_i), uri, tweet]) + u'\n')

out.close()
