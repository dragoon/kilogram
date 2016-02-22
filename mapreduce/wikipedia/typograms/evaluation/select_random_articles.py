import random
import string
import os
import shutil


out = 'EVAL'
os.makedirs(out)
# 20 random articles:
for _ in range(20):
    first_letter = random.choice('AB')
    second_letter = random.choice(string.uppercase)
    letter_path = first_letter+second_letter
    rand_number = random.randint(0, 9999)
    path = letter_path + '/wiki_'+str(rand_number)
    print(path)
    shutil.copy(path, out+'/'+letter_path+str(rand_number))
