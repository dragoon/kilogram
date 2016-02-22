from __future__ import division
import sys

organic_file = open(sys.argv[1]).read().splitlines()
predicted_file = open(sys.argv[2]).read().splitlines()

organic_data = [x.split('\t') for x in organic_file]
predicted_data = [x.split('\t') for x in predicted_file]


overlap_len = len(set(organic_data).intersection(predicted_data))

print('Overlap:', overlap_len,
      'Percentage:', overlap_len/min(len(organic_data), len(predicted_data)))
