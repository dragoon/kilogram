from __future__ import division
import sys

organic_file = open(sys.argv[1]).read().splitlines()
predicted_file = open(sys.argv[2]).read().splitlines()

organic_data = [tuple(x.split('\t')) for x in organic_file]
predicted_data = [tuple(x.split('\t')) for x in predicted_file]


predicted_data_set = set(predicted_data)
overlap_len = len(predicted_data_set.intersection(organic_data))

print('Overlap:', overlap_len,
      'Percentage:', overlap_len/min(len(organic_data), len(predicted_data)))


for line in organic_data:
    label, uri, sentence = line
    if label.lower().replace(' ', '_') in uri.lower() and line not in predicted_data_set:
        predicted_data.append((label, uri, sentence))


for line in predicted_data:
    print('\t'.join(line))
