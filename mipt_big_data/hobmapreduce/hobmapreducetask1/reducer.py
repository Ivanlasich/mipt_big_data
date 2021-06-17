#! /usr/bin/env python3

import sys

current_key = None
sum_count = 0
to_print = 1

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    	
    if current_key != key:
        if current_key and to_print==1:
            print("{}\t{}".format(current_key, sum_count))
        
        to_print = 1
        sum_count = 0
        current_key = key

    if count==-1:
        to_print = 0

    sum_count += count

if current_key and to_print==1:
    print("{}\t{}".format(current_key, sum_count))
