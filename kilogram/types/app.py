#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.types.prediction import predict_types
from kilogram import NgramService

NgramService.configure([], mongo_host=('localhost', '27017'), hbase_host=('diufpc301', 9090))

app = Flask(__name__)

@app.route('/', methods=['GET'])
def predict():
    context = request.args.get('context').strip().split()
    return jsonify({'types': predict_types(context)})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
