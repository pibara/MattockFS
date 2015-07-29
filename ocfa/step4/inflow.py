#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0import os
#
#This script tries to overlay two partial probability density functions in order to show balance or inbalance
#between ficticious disk cache inflow and outflow for an OCFA system based on the timing dump from step 1.
#We do this in one minute time granularity.
#
import os
import json
import sys
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot

#Mostly the graph plotting function from step 3 here. Look there for more code comments.
def plotresult (arrin,arrout,out,name):
    minslot=None
    maxslot=None
    for key in arrin:
        if minslot == None:
            minslot=key
            maxslot=key
        else:
            if minslot > key:
                minslot = key
            if maxslot < key:
                maxslot = key
    x=[]
    y=[]
    y2=[]
    totalcount=0
    totalcount2=0
    for lkey in range(int(minslot*10),int(maxslot*10)+1):
        key=lkey*1.0/10
        if key in arrin:
            totalcount += arrin[key]
        if key in arrout:
            totalcount2 += arrout[key]
    for lkey in range(int(minslot*10),int(maxslot*10)+1):
        key=lkey*1.0/10
        x.append(key)
        if key in arrin:
            y.append(1.0*arrin[key]/totalcount) 
        else:
            y.append(0.0)
        if key in arrout:
            y2.append(-1.0*arrout[key]/totalcount2)
        else:
            y2.append(0.0)
    y[0]=0.0
    y2[0]=0.0
    fig=matplotlib.pyplot.figure()
    matplotlib.pyplot.plot(x,y)
    matplotlib.pyplot.plot(x,y2)
    matplotlib.pyplot.fill_between(x,0,y,facecolor='blue', alpha=0.5)
    matplotlib.pyplot.fill_between(x,y2,0,facecolor='green', alpha=0.5)
    matplotlib.pyplot.xlabel(r'$log_{10}(\Delta c)$')
    matplotlib.pyplot.ylabel('p')
    matplotlib.pyplot.title('Probability distribution of ' + name)
    fig.savefig(out)

if len(sys.argv) < 2:
    print "usage :\n\t\t" + sys.argv[0] + " <inputfile>"
    sys.exit()

jsonstream = open(sys.argv[1])
out = sys.argv[1] + "_inflow.eps"
inflow  = {} 
outflow = {}
for jsonline in jsonstream:
    obj = json.loads(jsonline)
    osize = int(obj["a_size"])
    if (len(obj["jobs"]) > 0): #Just to make our code more robust here.
        #quick access to first job start and last job end.
        starttime = obj["jobs"][0]["time_start"]/60
        endtime = obj["jobs"][-1]["time_stop"]/60
        #Update inflow histogram value for starttime
        if starttime in inflow:
            inflow[starttime] += osize
        else:
            inflow[starttime] = osize
        #Update inflow histogram value for endtime
        if endtime in outflow:
            outflow[endtime] += osize
        else:
            outflow[endtime] = osize
        #Make sure all times pressent are present in both histos
        if not starttime in outflow:
            outflow[starttime] = 0.0 
        if not endtime in inflow:
            inflow[endtime] = 0.0

#Adjust inflow and outflow values
for eventtime in inflow:
    incount = inflow[eventtime]
    outcount = outflow[eventtime]
    #If both inflow and outflow have a value, we adjust our net in/out flows.
    if (incount !=0) and (outcount != 0):
        #Always extract the lowest from the highest number and set the lowest to zero.
        if (incount > outcount):
            inflow[eventtime] -= outcount
            outflow[eventtime] = 0.0
        if (incount <= outcount):
            outflow[eventtime] -= incount
            inflow[eventtime] = 0.0 

#Create two new histograms for order of magnitude of in/out-flow.
omspreadin = {}
omspreadout = {}
icount=0

#Fill the histograms.
for eventtime in inflow:
    ombytes = 0.0
    if inflow[eventtime] > 0:
        ombytes = int(math.log(inflow[eventtime],10.0)*10)*1.0/10
    if ombytes > 0 and ombytes in omspreadin:
        omspreadin[ombytes] += 1   
    else:
        omspreadin[ombytes] = 1
    ombytes = 0.0
    if outflow[eventtime] > 0:
        ombytes = int(math.log(outflow[eventtime],10.0)*10)*1.0/10
    if ombytes > 0 and ombytes in omspreadout:
        omspreadout[ombytes] += 1 
    else:
        omspreadout[ombytes] = 1
    
#Plot the result to an eps file.
plotresult (omspreadin,omspreadout,out,r"$\Delta C$ for $\Delta t = 60$")
        
