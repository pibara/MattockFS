#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0import os
#
#This script generates a set of four probability density graphs for an OCFA timing dump from step1.
#This is done for the combinations:
#  *  inter-job timing / job-count
#  *  inter-job timing /  data-access byte-count
#  *  first-last timing / evidence-count
#  *  first-last timing / data byte count
# 
import json
import sys
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot

#Simple helper function for generating a plot
def plotresult (arr,out,name,totalcount):
    #Find the lowest and highest number in our arr histo.
    minslot=None
    maxslot=None
    for key in arr:
        if minslot == None:
            minslot=key
            maxslot=key
        else:
            if minslot > key:
                minslot = key
            if maxslot < key:
                maxslot = key
    #Start off with empty plot sets.
    x=[]
    y=[]
    y2=[]
    ycum=0 #Cumultative probability.
    #Go though the range in steps of 0.1
    for lkey in range(int(minslot*10),int(maxslot*10)+1):
        key=lkey*1.0/10
        #Append our x.
        x.append(key)
        if key in arr:
            #Append our scaled y
            y.append(1.0*arr[key]/totalcount)
            #Update our comultative probability
            ycum += 1.0*arr[key]/totalcount
        else:
            #Or append zero if not defined in histo
            y.append(0.0)
        #Append to our comultative probability function
        y2.append(ycum)
    #Create our plot.
    fig=matplotlib.pyplot.figure()
    matplotlib.pyplot.plot(x,y)
    matplotlib.pyplot.plot(x,y2)
    matplotlib.pyplot.fill_between(x,0,y,facecolor='blue', alpha=0.5)
    matplotlib.pyplot.xlabel(r'$log_{10}(\Delta t)$')
    matplotlib.pyplot.ylabel('p')
    matplotlib.pyplot.title('Probability distribution of ' + name)
    #And write it to an eps file for inclusion in our OCFA appendix.
    fig.savefig(out)

if len(sys.argv) < 2:
    print "usage :\n\t\t" + sys.argv[0] + " <inputfile>"
    sys.exit()

jsonstream = open(sys.argv[1])
out1 = sys.argv[1] + "_startstop_by_size.eps"
out2 = sys.argv[1] + "_startstop_by_count.eps"
out3 = sys.argv[1] + "_prevnext_by_size.eps"
out4 = sys.argv[1] + "_prevnext_by_count.eps"
#Keep up with each of our four target comultative value
totalsize = 0
totalsize2 = 0
totalcount = 0
totalcount2 =0
#Start off with empty histograms
bysize  = {}
bycount = {}
bysize2  = {}
bycount2 = {}
for jsonline in jsonstream:
    obj = json.loads(jsonline)
    osize = int(obj["a_size"])
    starttime=None
    endtime=None
    #Go trough each job
    for job in obj["jobs"]:
        curmodule = job["module"]
        if starttime == None:
            #Take job start time of first job as evidence start time
            starttime = job["time_start"]
        else :
            #Determine the order of magnitude histogram box to put the interjob difftime in. 
            omdifftime2 = 0.0
            if job["time_start"] > endtime:
                 omdifftime2 = int(math.log((job["time_start"] - endtime) * 1.0,10.0)*10)*1.0/10
            #Update both the comultative bytes and job counts for this order of magnitude slot.
            if not omdifftime2 in bysize2:
                 bysize2[omdifftime2]  = osize
                 bycount2[omdifftime2] = 1
            else:
                bysize2[omdifftime2]  += osize
                bycount2[omdifftime2] += 1
            #Update the per job totals
            totalsize2 += osize
            totalcount2 += 1
        endtime = job["time_stop"]
    if endtime != None:
        #Take the order of mamagnitude slot of the first-last job time difference.
        omdifftime = 0.0
        if (endtime > starttime) :
            omdifftime = int(math.log((endtime - starttime) * 1.0,10.0)*10)*1.0/10
        #Update both the comultative bytes and evidence counts for this order of magnitude slot.
        if not omdifftime in bysize:
            bysize[omdifftime]  = osize
            bycount[omdifftime] = 1
        else:
            bysize[omdifftime]  += osize
            bycount[omdifftime] += 1
        #Update the per evidence totals
        totalsize += osize
        totalcount += 1
#Plot all the results in four seperate eps graphs.
plotresult (bysize,out1,"start/stop time difference by bytecount",totalsize)
plotresult (bycount,out2,"start/stop time difference by evidence count",totalcount)
plotresult (bysize2,out3,"inter-module time difference by bytecount",totalsize2)
plotresult (bycount2,out4,"inter-module time difference by forward count",totalcount2)
        
