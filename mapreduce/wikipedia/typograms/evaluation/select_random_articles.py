import random
import string
import os
import shutil
import argparse


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-n', dest='article_num', action='store', required=False, type=int,
                    default=20, help='number of articles to select')
parser.add_argument('--output_dir', required=True,
                    help='path to the directory to output random articles')

args = parser.parse_args()



out = args.output_dir
os.makedirs(out)
# 20 random articles:
for _ in range(args.article_num):
    first_letter = random.choice('AB')
    second_letter = random.choice(string.uppercase)
    letter_path = first_letter+second_letter
    rand_number = random.randint(0, 9999)
    path = letter_path + '/wiki_'+str(rand_number)
    print(path)
    shutil.copy(path, out+'/'+letter_path+str(rand_number))
