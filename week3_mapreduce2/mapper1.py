#!/usr/bin/python
"""reducer.py"""

import sys
#import random
#import string

current_tag, tag_count, current_year = None, 0, 0

#print("**********************************")
# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    #print("****")
    #print(line)
    #print("***")
    year, tag, count  = line.split('\t', 2)

    if (year != "2010") & (year != "2016"):
        continue
        #print(count)
    print(year, count, tag, sep = "\t")
