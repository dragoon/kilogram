#!/usr/bin/env python
"""
./flask_service.py unambig_labels_file.txt
"""

from flask import Flask, jsonify, request

from functools import partial
from kilogram.entity_linking.unambig_labels.link_generators import generate_links, unambig_generator,\
    get_unambiguous_labels

__author__ = 'dragoon'

import sys

unambiguous_labels = get_unambiguous_labels(sys.argv[1])
unambig_generator_local = partial(unambig_generator, unambiguous_labels=unambiguous_labels)
linker = partial(generate_links, generators=[unambig_generator_local])

app = Flask(__name__)

@app.route('/entity-linking/a2kb/unambig', methods=['POST'])
def link():
    result = request.get_json(force=True)
    text = result['text']
    mentions = []
    cur_index = 0
    for token, uri, orig_sentence in linker(text):
        start_i = text.index(token, cur_index)
        mentions.append({'name': token, 'uri': uri, 'start': start_i, 'end': start_i+len(token)})
        cur_index = start_i + len(token)

    return jsonify({'mentions': mentions})


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
