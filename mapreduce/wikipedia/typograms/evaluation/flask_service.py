#!/usr/bin/env python
"""
./flask_service.py unambig_labels_file.txt
"""

from flask import Flask, jsonify, request

from functools import partial
from .link_generators import generate_organic_precise_plus, generate_links, unambig_generator
from .evaluate import get_unambiguous_labels

__author__ = 'dragoon'

import sys

unambiguous_labels = get_unambiguous_labels(sys.argv[1])
unambig_generator_local = partial(unambig_generator, unambiguous_labels=unambiguous_labels)
linker = partial(generate_organic_precise_plus,
                 evaluator=partial(generate_links, generators=[unambig_generator_local]))

app = Flask(__name__)

@app.route('/entity-linking-unambig', methods=['GET'])
def link():
    text = request.args.get('text', '')

    return jsonify({'entities': list(linker(text))})


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
