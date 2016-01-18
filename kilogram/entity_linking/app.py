#!/usr/bin/env python
"""
./app.py --dbpedia-data-dir /home/roman/dbpedia --ner-host diufpc54.unifr.ch --types-table typogram --hbase-host diufpc304
"""
import argparse
import os.path

from flask import Flask, jsonify, request

import kilogram
from kilogram.dataset.entity_linking.gerbil import DataSet
from kilogram.entity_linking import syntactic_subsumption
from kilogram.dataset.dbpedia import DBPediaOntology, NgramEntityResolver
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dbpedia-data-dir', dest='dbpedia_data_dir', action='store', required=True,
                    help='DBpedia data directory')
parser.add_argument('--ner-host', dest='ner_host', action='store', required=True,
                    help='Hostname of the server where Stanford NER is running')
parser.add_argument('--types-table', dest='types_table', action='store', required=True,
                    help='Typed N-gram table in HBase')
parser.add_argument('--hbase-host', dest='hbase_host', action='store', required=True,
                    help='HBase gateway host')
parser.add_argument('--hbase-port', dest='hbase_port', action='store',
                    default='9090', help='HBase gateway host')


args = parser.parse_args()

NgramService.configure(hbase_host=(args.hbase_host, args.hbase_port))
kilogram.NER_HOSTNAME = args.ner_host
ner = NgramEntityResolver(os.path.join(args.dbpedia_data_dir, "dbpedia_data.txt"),
                          os.path.join(args.dbpedia_data_dir, "dbpedia_uri_excludes.txt"),
                          os.path.join(args.dbpedia_data_dir, "dbpedia_lower_includes.txt"),
                          os.path.join(args.dbpedia_data_dir, "dbpedia_2015-04.owl"))


dbpedia_ontology = DBPediaOntology(os.path.join(args.dbpedia_data_dir, "dbpedia_2015-04.owl"))
ngram_predictor = NgramTypePredictor(args.types_table, dbpedia_ontology)

app = Flask(__name__)


@app.route('/entity-linking/d2kb/prior', methods=['POST'])
def d2kb_prior():
    result = request.get_json(force=True)
    candidates = DataSet(result['text'], result['mentions'], ner).candidates
    syntactic_subsumption(candidates)
    new_mentions = []
    for mention, candidate in zip(result['mentions'], candidates):
        mention['uri'] = candidate.get_max_uri()
        if mention['uri'] is not None:
            new_mentions.append(mention)
    return jsonify({'mentions': new_mentions})


@app.route('/entity-linking/d2kb/prior-typed', methods=['POST'])
def d2kb_prior_types():
    result = request.get_json(force=True)
    candidates = DataSet(result['text'], result['mentions'], ner).candidates
    syntactic_subsumption(candidates)
    for candidate in candidates:
        candidate.init_context_types(ngram_predictor)
    new_mentions = []
    for mention, candidate in zip(result['mentions'], candidates):
        context_types_list = [c.context_types for c in candidates
                              if c.context_types and c.cand_string == candidate.cand_string]
        if context_types_list:
            mention['uri'] = candidate.get_max_typed_uri(context_types_list)
        else:
            mention['uri'] = candidate.get_max_uri()
        new_mentions.append(mention)
    return jsonify({'mentions': new_mentions})


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
