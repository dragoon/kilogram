#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram.entity_types.prediction import TypePredictor
from kilogram import NgramService

NgramService.configure(hbase_host=('diufpc301', 9090))

app = Flask(__name__)
dbpedia_ontology = DBPediaOntology('dbpedia_2014.owl')
predictor = TypePredictor("ngram_types", dbpedia_ontology)

@app.route('/', methods=['GET'])
def predict():
    context = request.args.get('context').strip().split()
    return jsonify({'types': predictor.predict_types(context)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
