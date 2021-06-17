#! /usr/bin/env python3

import sys

current_key = None
sum_count = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            new_answ = current_key.split("_")
            print("{}\t{}\t{}".format(new_answ[0], new_answ[1], sum_count))

        sum_count = 0
        current_key = key

    sum_count += count

if current_key:
    new_answ = current_key.split("_")
    print("{}\t{}\t{}".format(new_answ[0], new_answ[1], sum_count))
