import sys
from kilogram import ListPacker
from kilogram.lang.tokenize import default_tokenize_func, tokenize_possessive

# Split each line into words
def unpack_achors(line):
    label, uri_list = line.split('\t')
    # tokenize for commas
    label = ' '.join(tokenize_possessive(default_tokenize_func(label)))
    # should be only one
    uri_counts = ListPacker.unpack(uri_list)
    total_count = sum(int(c) for _, c in uri_counts)
    print(label + '\t' + 'uri\t' + str(total_count))


for line in sys.stdin:
    unpack_achors(line)
