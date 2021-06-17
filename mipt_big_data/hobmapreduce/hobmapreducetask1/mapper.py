#! /usr/bin/env python3

import random
import sys
import re

sys.stdin = open(sys.stdin.fileno(), encoding='utf-8')

for line in sys.stdin:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    words = re.split('\W*\s+\W*', text, re.UNICODE)
    for word in words:
    	if len(word)>=6 and len(word)<=9:
    		if(word.lower() == word):
      			print("{}\t{}".format(word.lower(), -1))
    		else:
      			print("{}\t{}".format(word.lower(), 1))
	

