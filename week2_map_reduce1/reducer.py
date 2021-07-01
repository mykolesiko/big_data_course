#!/usr/bin/python
"""reducer.py"""

import sys
import random


KEY_LEN = 10

# input comes from STDIN (standard input)
#random.seed(10)
words = []
for line in sys.stdin:
    line = line.strip()
    #print(line)
    key, word  = line.split('\t', 1)
    words.append(word)
while len(words) > 0:
    len_temp =  random.randint(1, min(5, len(words)))
    #print(len_temp)
    ind_temp = random.sample(range(0,len(words)), k = len_temp)
    #print(ind_temp)
    list_temp = [words[ind] for ind in ind_temp]
    for el in list_temp:
        words.remove(el)
    print(",".join(list_temp))
    #print('%s\t%s' % (Жkey, word))
