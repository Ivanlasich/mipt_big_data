#! /usr/bin/env python3

import random
import sys
import re

sys.stdin = open(sys.stdin.fileno(), encoding='utf-8')

for line in sys.stdin:
    line = line.splitlines()
    line = line[0]
    try:
        words = re.split(" ", line, flags=re.UNICODE)
    except ValueError as e:
        continue
    if len(words)>6 and (words[4]+' '+words[5]+' '+words[6])== "issued server command:":
	#print(words[0])
        print("{}_{}\t{}".format(words[0].split(".")[0][1:], words[7], 1))

