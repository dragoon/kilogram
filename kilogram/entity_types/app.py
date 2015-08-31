#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService
from kilogram.lang.unicode import strip_unicode

NgramService.configure(hbase_host=('diufpc304', 9090))

app = Flask(__name__)
dbpedia_ontology = DBPediaOntology('dbpedia_2015-04.owl')
ngram_predictor = NgramTypePredictor("ngram_types", dbpedia_ontology,
                          '/home/roman/notebooks/kilogram/mapreduce/dbpedia_types.dbm')


@app.route('/predict/types/context', methods=['GET'])
def predict_ngram_from_context():
    context = strip_unicode(request.args.get('context').strip()).split()
    return jsonify({'types': ngram_predictor.predict_types(context)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
