#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0
#
# This script uses the 'sorted' output of the script eventdump.py and uses it to create a simple plot
# of the probability of the size requirements for a ficticious perfect caching system.
import sys
import math
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot

cursecond = 0
curvirtcachesize = 0
totalseconds = 0
omhisto = {} #An order of magnitude histogram for our ficticious cache need.
minslot = None
maxslot = None
for line in sys.stdin:
    second,operator,datasize = line.split()
    second = int(second)
    if cursecond >= 0 and second != cursecond: #At the next second, look at how much happened this second.
        totalseconds += 1
        if curvirtcachesize > 0:
            #Calculate a slotted order of magnitude for our chache size to use in our histogram.
            #we use a 10 log for this, rounded to a single digit fixed point digit.
            omcurvirtcachesize = int(math.log(curvirtcachesize * 1.0,10.0)*10)*1.0/10
            #Set or adjust minslot/maxslot for our histo.
            if minslot == None:
                minslot = omcurvirtcachesize
                maxslot = omcurvirtcachesize
            else:
                if omcurvirtcachesize < minslot:
                    minslot = omcurvirtcachesize
                if omcurvirtcachesize > maxslot:
                    maxslot = omcurvirtcachesize
            #Set or update our histogram
            if omcurvirtcachesize in omhisto:
                omhisto[omcurvirtcachesize] += 1
            else :
                omhisto[omcurvirtcachesize] = 1
    #Do the updating for our current second.
    cursecond = second
    if operator == '+':
        curvirtcachesize += int(datasize)
    else:
        curvirtcachesize -= int(datasize)

#Start off with empyy sets.
x=[]
y=[]
y2=[]
ycum=0 
#Walk trough the full range of our histo.
for lkey in range(int(minslot*10),int(maxslot*10)+1):
    key=lkey*1.0/10
    #Add an element to the x axis.
    x.append(key)
    if key in omhisto:
        #place our histo value as probability density on the y axis.
        y.append(1.0*omhisto[key]/totalseconds)
        #Update the comultative probability density.
        ycum += 1.0*omhisto[key]/totalseconds
    else:
        #Or zero if not defined in our histo.
        y.append(0.0)
    #Add comultative probability to our second set of y values
    y2.append(ycum)
#Plot the probability density and comultative probability density .
fig=matplotlib.pyplot.figure()    
matplotlib.pyplot.plot(x,y)
matplotlib.pyplot.plot(x,y2)
matplotlib.pyplot.fill_between(x,0,y,facecolor='blue', alpha=0.5)
matplotlib.pyplot.xlabel(r'$\log_{10}(C)$')
matplotlib.pyplot.ylabel('p')
matplotlib.pyplot.title('Probability distribution of $\log_{10}$ of cache demand')
#Save the plot to an eps file to be used in our OCFA appendix.
fig.savefig(sys.argv[1])


