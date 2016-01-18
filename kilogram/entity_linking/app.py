#!/usr/bin/env python
"""
./app.py typogram
"""

import sys

from flask import Flask, jsonify, request

import kilogram
from kilogram.dataset.entity_linking.gerbil import DataSet
from kilogram.entity_linking import syntactic_subsumption
from kilogram.dataset.dbpedia import DBPediaOntology, NgramEntityResolver
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService

NgramService.configure(hbase_host=('diufpc304', '9090'))
kilogram.NER_HOSTNAME = 'diufpc54.unifr.ch'
ner = NgramEntityResolver("/Users/dragoon/Downloads/dbpedia/dbpedia_data.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_uri_excludes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_lower_includes.txt",
                          "/Users/dragoon/Downloads/dbpedia/dbpedia_2015-04.owl")


dbpedia_ontology = DBPediaOntology('dbpedia_2015-04.owl')
ngram_predictor = NgramTypePredictor(sys.argv[1], dbpedia_ontology)

app = Flask(__name__)


@app.route('/entity-linking/d2kb/prior', methods=['POST'])
def d2kb_prior():
    result = request.get_json(force=True)
    candidates = DataSet(result['text'], result['mentions'], ner).candidates
    syntactic_subsumption(candidates)
    new_mentions = []
    for mention, candidate in zip(result['mentions'], candidates):
        mention['uri'] = candidate.get_max_uri()
        new_mentions.append(mention)
    return jsonify(new_mentions)


@app.route('/entity-linking/d2kb/prior-typed', methods=['POST'])
def d2kb_prior():
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
    return jsonify(new_mentions)


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
