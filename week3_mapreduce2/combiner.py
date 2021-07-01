#!/usr/bin/python
"""reducer.py"""

import sys
import random
import string

current_tag, tag_count, current_year = None, 0, 0

#print("**********************************")
# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    #print("****")
    #print(line)
    #print("***")
    year, tag, count  = line.split('\t', 2)

    if (year == current_year) & (tag == current_tag):
        tag_count += int(count)
        #print(count)
    else:
        if current_tag:
            print(current_year, current_tag, tag_count, sep = "\t")
        current_year, current_tag, tag_count =  year, tag, int(count)
        
if current_tag:
    print(year, tag, count, sep = "\t")    

