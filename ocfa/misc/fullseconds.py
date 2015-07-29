#!/usr/bin/python
import sys

amap = {}
cursecond = 0
curvirtcachesize = 0
fullcache = False
firstsec = -1
for line in sys.stdin:
    second,operator,datasize = line.split()
    second = int(second)
    if firstsec == -1:
        firstsec = second
    if cursecond >= 0 and second != cursecond:
        if fullcache == False:
            if curvirtcachesize > 1073741824:
                amap[second] = 1
                fullcache = True
        else:
            if curvirtcachesize <= 1073741824:
                amap[second] = -1
                fullcache = False
    cursecond = second
    if operator == '+':
        curvirtcachesize += int(datasize)
    else:
        curvirtcachesize -= int(datasize)

fullc = False
for second in range(firstsec,second+1):
    if second in amap:
        if amap[second] == 1:
            fullc = True
        else:
            fullc = False
    if fullc:
        print second
