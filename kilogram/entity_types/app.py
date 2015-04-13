#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.entity_types.word2vec.model import TypePredictionModel
from kilogram.dataset.dbpedia import DBPediaOntology
from kilogram.entity_types.prediction import NgramTypePredictor
from kilogram import NgramService
from kilogram.lang.unicode import strip_unicode

NgramService.configure(hbase_host=('diufpc301', 9090))

app = Flask(__name__)
dbpedia_ontology = DBPediaOntology('dbpedia_2014.owl')
ngram_predictor = NgramTypePredictor("ngram_types", dbpedia_ontology,
                          '/home/roman/notebooks/kilogram/mapreduce/dbpedia_types.dbm')
deep_predictor = TypePredictionModel('/home/roman/berkeleylm/300features_40minwords_10context',
                                     '/home/roman/notebooks/kilogram/kilogram/entity_types/tests/type_prediction_train.tsv',
                                     type_hierarchy=dbpedia_ontology)


@app.route('/predict/types/context', methods=['GET'])
def predict_ngram_from_context():
    context = strip_unicode(request.args.get('context').strip()).split()
    return jsonify({'types': ngram_predictor.predict_types(context)})

@app.route('/predict/types/word/linear', methods=['GET'])
def deep_predict_linear():
    ngram = strip_unicode(request.args.get('ngram').strip())
    return jsonify({'types': deep_predictor.predict_types_linear(ngram)})

@app.route('/predict/types/word/raw', methods=['GET'])
def deep_predict_raw():
    ngram = strip_unicode(request.args.get('ngram').strip())
    return jsonify({'types': deep_predictor.predict_types_similarity(ngram)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
