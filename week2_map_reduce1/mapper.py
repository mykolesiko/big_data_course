#!/usr/bin/python
"""mapper.py"""

import sys
import random
import string

KEY_LEN = 10

# input comes from STDIN (standard input)
#random.seed(6)
for word in sys.stdin:
    word = word.strip()
    key = ""
    for i in range(KEY_LEN):
        key = key + random.choice(string.ascii_letters)
    key = key + word
    print('%s\t%s' % (key, word))
