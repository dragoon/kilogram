#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.dataset.dbpedia import get_dbpedia_type_hierarchy
from kilogram.entity_types.prediction import predict_types
from kilogram import NgramService

NgramService.configure(hbase_host=('diufpc301', 9090))

app = Flask(__name__)
dbpedia_type_hierarchy = get_dbpedia_type_hierarchy('dbpedia_2014.owl')

@app.route('/', methods=['GET'])
def predict():
    context = request.args.get('context').strip().split()
    return jsonify({'types': predict_types(context, dbpedia_type_hierarchy)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
