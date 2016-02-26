from __future__ import division
import argparse
from functools import partial
import os
import re
from .link_generators import generate_organic_links, generate_links, unambig_generator,\
    label_generator,  generate_organic_plus

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--gold-file', required=True,
                    help='path to the file with the ground truth')
parser.add_argument('--eval-dir', required=True,
                    help='path to the directory to evaluate')
parser.add_argument('--out-file', default="not_ranked.txt",
                    help='path to the file to output non-ranked items')

args = parser.parse_args()

SENT_STRIP_RE = re.compile(r'[\"\'\s]')


def get_unambiguous_labels(filename):
    unambiguous_labels = {}
    for line in open(filename, 'r'):
        label, uri = line.strip().split('\t')
        unambiguous_labels[label] = uri
    return unambiguous_labels


def get_gold_data():
    gold_data = {}
    for line in open(args.gold_file):
        correct, uri_equals, full_url, token, uri, orig_sentence = line.strip().split('\t')
        token = token.replace(" '", "'").replace("'s", "")
        sentence = SENT_STRIP_RE.sub("", orig_sentence)
        gold_data[(token, uri, sentence)] = int(correct)
    return gold_data

gold_data = get_gold_data()


def evaluate(eval_name, evaluator, eval_dir):
    """
    :param eval_name: name of the evaluator, can be anything
    :param evaluator: evaluator function (see examples)
    :return: precision
    """
    labels = []
    print('Evaluating:', eval_name)
    for filename in os.listdir(eval_dir):
        for line in open(args.eval_dir + '/' + filename):
            for token, uri, orig_sentence in evaluator(line):
                sentence = SENT_STRIP_RE.sub("", orig_sentence)
                token = token.replace(" '", "'").replace("'s", "")
                if (token, uri, sentence) in gold_data:
                    label = gold_data[(token, uri, sentence)]
                    labels.append(label)
                else:
                    not_ranked_file.write('\t'.join([token, uri, orig_sentence])+'\n')
    print('Precision:', sum(labels)/len(labels))
    print('Recall:', sum(labels)/total_correct)
    print('.')
    return sum(labels)/len(labels)


not_ranked_file = open(args.out_file, 'w')
total_correct = sum(gold_data.values())



print('Evaluating parameters...')

max_precision = precision = 0
max_filename = None

for filename in os.listdir('.'):
    if filename.startswith('unambiguous_labels'):
        print('Evaluating ' + filename)
        unambiguous_labels = get_unambiguous_labels(filename)
        unambig_generator = partial(unambig_generator, unambiguous_labels=unambiguous_labels)
        precision = evaluate('inferred',  partial(generate_links, generators=[unambig_generator]), args.eval_dir)
    if filename.startswith('unambiguous_percentile_labels'):
        print('Evaluating ' + filename)
        unambiguous_labels = get_unambiguous_labels(filename)
        unambig_generator = partial(unambig_generator, unambiguous_labels=unambiguous_labels)
        precision = evaluate('inferred',  partial(generate_links, generators=[unambig_generator]), args.eval_dir)
    if precision > max_precision:
        max_precision = precision
        max_filename = filename

unambiguous_labels = get_unambiguous_labels(max_filename)
unambig_generator = partial(unambig_generator, unambiguous_labels=unambiguous_labels)


evaluations = [('organic', generate_organic_links),
               ('organic-precise', generate_organic_plus),
               ('inferred', partial(generate_links, generators=[unambig_generator])),
               ('inferred + organic-precise', partial(generate_organic_plus,
                            evaluator=partial(generate_links, generators=[unambig_generator]))),
               ('labels', partial(generate_links, generators=[label_generator])),
               ('inferred + labels', partial(generate_links,
                                           generators=[unambig_generator, label_generator])),
               ('inferred + labels + organic-precise', partial(generate_organic_plus,
                            evaluator=partial(generate_links, generators=[unambig_generator, label_generator])))]


for eval_name, evaluator in evaluations:
    evaluate(eval_name, evaluator, args.eval_dir)

# evaluate spotlight
for spot_dir in os.listdir('./spotlight'):
    evaluate('spotlight', generate_organic_links, './spotlight' + spot_dir)

not_ranked_file.close()
