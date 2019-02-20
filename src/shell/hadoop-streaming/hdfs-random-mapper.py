#!/usr/bin/env python
# encoding: utf-8

import sys
import hashlib
import random
import Queue

K=int(sys.argv[1])
i=K
result=Queue.Queue()
for line in sys.stdin:
    line = line.strip()
    if result.qsize()<K :
        result.put(line)
    else:
        i=i+1
        if random.randint(0,i)==1:
            result.get()
            result.put(line)

while not result.empty():
    print result.get()