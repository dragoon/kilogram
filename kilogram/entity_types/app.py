#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram.entity_types.prediction import TypePredictor
from kilogram import NgramService
from kilogram.lang.unicode import strip_unicode

NgramService.configure(hbase_host=('diufpc301', 9090))

app = Flask(__name__)
dbpedia_ontology = DBPediaOntology('dbpedia_2014.owl')
predictor = TypePredictor("ngram_types", dbpedia_ontology,
                          '/home/roman/notebooks/kilogram/mapreduce/dbpedia_types.dbm',
                          word2vec_model_filename='/home/roman/berkeleylm/300features_40minwords_10context')

@app.route('/predict/types/context', methods=['GET'])
def predict():
    context = strip_unicode(request.args.get('context').strip()).split()
    return jsonify({'types': predictor.predict_types(context)})

@app.route('/predict/types/word', methods=['GET'])
def predict():
    ngram = strip_unicode(request.args.get('ngram').strip()).split()
    return jsonify({'types': predictor._predict_types_from_ngram(ngram)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
