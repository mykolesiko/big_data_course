#!/usr/bin/python
"""mapper.py"""

import sys
import random
#import string
from lxml import etree


TAGS = "Tags"
CREATION_DATE = "CreationDate"

# input comes from STDIN (standard input)
random.seed(6)
for line in sys.stdin:
    line = line.strip()
    if line[0:4] != "<row":
        continue
   # line1 = line[1: -1:1]
   # line1 = line1.replace(">", ";")
   # line1 = line1.replace("<", ";")
   # line = "<" + line1 + ">"
    #print(line)
    parser = etree.XMLParser(remove_blank_text=True)
    row = etree.fromstring(line, parser)
    if row.tag != "row":
        continue
    attributes = row.attrib

    date = attributes.get(CREATION_DATE)
    date1 = date[0:4]
    if (date1 != "2016") & (date1 != "2010"):
        continue
    tags = attributes.get(TAGS)
    if tags != None:
        tags = tags.strip('<>')
        tags_list = tags.split("><")
        #tags = tags.strip(',')
        #tags_list = tags.split("><")
        for tag in tags_list:
            print('%s\t%s\t%s' % (date1, tag, "1"))
