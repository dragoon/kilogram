from __future__ import division
import argparse
from functools import partial
import os
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


gold_data = {}

for line in open(args.gold_file):
    correct, uri_equals, full_url, token, uri, orig_sentence = line.strip().split('\t')
    token = token.replace(" '", "'").replace("'s", "")
    sentence = orig_sentence.replace('"', '').replace(" ", "")
    gold_data[(token, uri, sentence)] = int(correct)


not_ranked_file = open(args.out_file, 'w')
total_correct = sum(gold_data.values())

evaluations = [('organic', generate_organic_links),
               ('organic-precise', generate_organic_plus),
               ('inferred', partial(generate_links, generators=[unambig_generator])),
               ('inferred+organic', partial(generate_organic_plus,
                            evaluator=partial(generate_links, generators=[unambig_generator]))),
               ('labels', partial(generate_links, generators=[label_generator])),
               ('inferred+labels', partial(generate_links,
                                           generators=[unambig_generator, label_generator]))]

for eval_name, evaluator in evaluations:
    labels = []
    print('Evaluating:', eval_name)
    for filename in os.listdir(args.eval_dir):
        for line in open(args.eval_dir + '/' + filename):
            for token, uri, orig_sentence in evaluator(line):
                sentence = orig_sentence.replace('"', '').replace(" ", "")
                token = token.replace(" '", "'").replace("'s", "")
                if (token, uri, sentence) in gold_data:
                    label = gold_data[(token, uri, sentence)]
                    labels.append(label)
                else:
                    not_ranked_file.write('\t'.join([token, uri, orig_sentence])+'\n')
    print('Precision:', sum(labels)/len(labels))
    print('Recall:', sum(labels)/total_correct)
    print('.')

not_ranked_file.close()
