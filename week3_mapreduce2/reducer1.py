#!/usr/bin/python
"""reducer.py"""

import sys
#import random
#import string



#print("**********************************")
# input comes from STDIN (standard input)
count2016 = 0
count2010 = 0
for line in sys.stdin:
    line = line.strip()
    #print("****")
    #print(line)
    #print("***")
    year, count, tag  = line.split('\t', 2)
    if (year == '2016') & (count2016 < 10 ):
        count2016 += 1
        print(year, tag, count, sep = "\t")
    if (year == '2010') & (count2010 < 10):
        count2010 += 1
        print(year, tag, count, sep = "\t")
