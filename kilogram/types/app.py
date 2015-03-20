#!/usr/bin/env python
from flask import Flask, jsonify, request
from kilogram.types.prediction import predict_types

app = Flask(__name__)

@app.route('/', methods=['GET'])
def predict():
    context = request.args.get('context').strip().split()
    return jsonify({'types': predict_types(context)})


if __name__ == '__main__':
    app.run(debug=True)
