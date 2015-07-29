#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0 
#
#This litle script goes trough the timing event dump from step1 and dumps a data in/out event with a timestamp
#for each evidence json in the input.  
import os
import json
import sys
import math

if len(sys.argv) < 2:
    print "usage :\n\t\t" + sys.argv[0] + " <inputfile>"
    sys.exit()

jsonstream = open(sys.argv[1])
#Process each line in the input dump.
for jsonline in jsonstream:
    #Parse the JSON
    obj = json.loads(jsonline)
    #Fetch the data size from the JSON
    osize = int(obj["a_size"])
    lastmodule=None
    exittime=None
    for job in obj["jobs"]:
        curmodule = job["module"]
        if lastmodule == None
            #Only print start time for first module.
            print job["time_start"], "+", osize
            pass
        exittime = job["time_stop"]
        lastmodule = curmodule
    if lastmodule != None and exittime != None:
        #Print the exit time for the very last module only.
        print exittime, "-", osize
