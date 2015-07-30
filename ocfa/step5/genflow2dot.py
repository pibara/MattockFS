#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0 
#
#This script generates a simple dot file for the main event and/or data flows between modules in an OCFA run.
#Please take note of the following legenda:
# - Percentage of evidence level data bytes processed by module.
#    + Red   : > 1%
#    + Black : 0.25% ..1%
#    + Green : < <0.25% 
# - Percenhtage of evidence level data processed by module.
#    + Normal : > 4%
#    + Dashed : 1% .. 4%
#    + Dotted : < 1%
#
# Note that dotted green relations are not plotted in order to keep the resulting plot usefull.
import os
import json
import sys

def modtype(modname):
    if modname in ["carver","file","digest"]:
        return modname
    else:
        if modname in ["snorkelkickstart","e01","resubmitter","fsstat","mmls","tskfs"] :
            return "kick"
        if modname == "photorec":
            return "carver"
        else:
            return "other"

def creator(modname):
    if modname in ["carver","kick"]:
        return modname
    else:
        return "creator"


if len(sys.argv) < 2:
    print "usage :\n\t\t" + sys.argv[0] + " <inputfile>"
    sys.exit()

jsonstream = open(sys.argv[1])
#Keep track of two maps for the density of each arc.
arcs={}
arcs2={}
totalsize =0
totallines = 0
#A helper maps for using different shapes for modules in their role of creator.
for jsonline in jsonstream:
    obj = json.loads(jsonline)
    osize = int(obj["a_size"])
    lastmodule=None #Keep track of previous module
    jobcount = 0
    for job in obj["jobs"]:
        jobcount += 1
        curmodule = modtype(job["module"])
        if lastmodule != None:
            #Use a basic dot file line as key for our graph arc out of convenience.
            arc = '    "' + lastmodule + '"  ->  "' + curmodule + '"'
            #Update the arc metrics for this arc.
            if arc in arcs:
                arcs[arc] += osize
                arcs2[arc] += 1
            else:
                arcs[arc] = osize
                arcs2[arc] = 1
            #And update the last module for the next job to be procesed.
            lastmodule = curmodule
        else:
            #Use a special name for the creator role.
            lastmodule = creator(curmodule)
    #Only update the total counts if there was at least one job.
    if jobcount > 1:
        totalsize += osize
        totallines +=1 

#Write a dotfile start to stdout.
print "digraph modules {"

#Now print all the arcs, given the above legenda.
for arc in arcs:
    percentage = ((arcs[arc]*10000/totalsize)*1.0)/100
    percentage2 = ((arcs2[arc]*10000/totallines)*1.0)/100
    if percentage >= 0.25 or percentage2 >= 1:
        if percentage < 0.25:
            if percentage2 > 4:
                print arc + ' [ label="' + str(percentage) + '",color=green ] ;'
            else:
                print arc + ' [ label="' + str(percentage) + '",color=green, style=dashed ] ;'
        else:
            if percentage < 1:
                if percentage2 > 4:
                    print arc + ' [ label="' + str(percentage) + '"];'
                else:
                    if percentage2 > 1:
                        print arc + ' [ label="' + str(percentage) + '", style=dashed ] ;'
                    else:
                        print arc + ' [ label="' + str(percentage) + '",style=dotted ] ;'
            else:
                if percentage2 > 4:
                    print arc + ' [ label="' + str(percentage) + '", color=red ] ;'
                else:
                    if percentage2 > 1:
                        print arc + ' [ label="' + str(percentage) + '", color=red, style=dashed ] ;'
                    else:
                        print arc + ' [ label="' + str(percentage) + '", color=red, style=dotted ] ;'
#End of the dot file
print "}"


