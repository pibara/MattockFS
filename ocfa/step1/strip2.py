#!/usr/bin/python
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0 
#
import os
import json
import sys
import calendar
import dateutil.parser
try: 
  import xml.etree.cElementTree as ET
except:
  import xml.etree.ElementTree as ET

if len(sys.argv) < 3:
    print "usage:   ./strip.py <inputfile> <outputfile>"
    sys.exit()

infile = sys.argv[1]
outfile = sys.argv[2] 
#Helper constant for Java API bug workaround.
twelvehour=3600*12

#This is a coroutine for extracting individual module info from an OCFA evidence XML trace BLOB.
def getmoduletimes(root, findsize):
    prevrouterstop=None  #At what time did the previous router send forward the evidence entity to the module.
    modulestart=None     #At what time did the current module start
    modulestop=None      #At what time did the current module stop
    module= None         #Name of the current module
    isjava=False         #If the module used the Java API, there is a timestamp bug we must work around.
    datasize=None        #Size of the data chunk of this evidence entity.
    needsize = findsize  #If requested, we look for the first and only the first occurence of 'size' meta data.
    #Iterate all the jobs in the evidence XML trace.
    for job in root.findall("job"):
        #We are only interested in traces that have both a start and a stop time.
        if ("stime" in job.attrib) and ("etime" in job.attrib):
            modorrouter = None  #More on this one later.
            modjava=False       #Start off asuming the module does not use the Java API
            for modinst in job.findall("moduleinstance"):
                modorrouter = modinst.attrib["module"]                #Extract module name.
                modjava = (modinst.attrib["instance"][:4] == 'java')  #Check for Java API (with implied timestamp bug)
            #The router isn't really a module. Its a part of the OCFA architecture responsible for forwarding
            #the evidence data chunk and its meta data to the next module. There thuss will be a router job between
            #every two module jobs in the XML trace.
            #Note that we won't yield info on hte final module. Its the DSM normaly, only concerned with meta-data
            #so much less relevant to the subject of our dissertation.  
            if modorrouter == "router": 
                #Extract the stop time of this router from the job trace.
                thisrouterstop = utime=calendar.timegm(dateutil.parser.parse(job.attrib["etime"]).utctimetuple())
                #If this job was created using the JAVA OCFA API, try to work around bogus timestamps.
                #This wont be 100% waterproof, but should eliminate most bad timestamps.
                if isjava:
                    if prevrouterstop == None:
                        #No router before us, so no reference to check if the start time is bogus.
                        #We can still check if the endtime is bogus by comparing it to the start time though.
                        if modulestop < modulestart:
                            #Adjust the endtime. We know the start time must be OK now also.
                            modulestop += twelvehour
                        else:
                            #If the module endtime isn't found to be bogus compared to its own starttime,
                            #lets compare it to the time of the next router.
                            #If we jump (over) twelve hours to the next router, most probably the endtime is bogus. 
                            #This is a best guess, this is were we may have false positives!
                            if (thisrouterstop - modulestop) >= twelvehour:
                                modulestop += twelvehour
                            #We do a best guess check if the start time is bogus based on presence of (over) twelve 
                            #hours between start and stop. Again, this is were we may have false positives!
                            if (modulestop - modulestart) >= twelvehour:
                                modulestart += twelvehour
                    else:
                        #Ok, this isn't the first job, lets use the previous router stop time to see if the start time
                        #is bogus.
                        if modulestart < prevrouterstop:
                            modulestart += twelvehour
                        #Now after we know for sure the (possibly adjusted) start time isn't bogus, or it would have 
                        #jumped back in time, lets see if the end time may be bogus in that way.
                        if modulestop < modulestart:
                            modulestop += twelvehour
                #Return module name and its start and stop time.
                yield [module,modulestart,modulestop,datasize]
                #Update the previous router time for the next loop.
                prevrouterstop = thisrouterstop
                #Do sme post module cleanup.
                modulestart=None
                modulestop=None
                module= None
                isjava=False
                datasize =None
            else:
                if modorrouter != None: #Not a router, so we have a normal module.
                    module=modorrouter #Remember the module name
                    #Remember module timing.
                    modulestart=utime=calendar.timegm(dateutil.parser.parse(job.attrib["stime"]).utctimetuple()) 
                    modulestop=utime=calendar.timegm(dateutil.parser.parse(job.attrib["etime"]).utctimetuple())
                    #Remember if this module uses the Java OCFA API
                    isjava=modjava
                    #If requested by the caller, extract the evidence data size.
                    if needsize:
                        for meta in job.findall("meta"):
                            if  meta.attrib["name"] == "size":
                                datasize = meta[0].text
                                needsize = False
        
        
        

#use a special value indicating there is no smallest timestamp yet.
min_utime=-1
print "Extracting minimum timestamp from input file"
#Open our input file using zcat for decompression.
xmlstream = os.popen("/bin/zcat "+infile,"r")
for xmlcandidate in xmlstream:
        #The evidence xml is contained in COPY lines in the mysql ascii dump, check if we have such a line.
        xmlstart = xmlcandidate.find("<evidence ")
        if xmlstart > 0:
            #First extract only the xml part from the COPY line, than unescape the newlines to restore XML BLOB
            xmlsize = xmlcandidate.find("</evidence>") + 11
            xmlrecord = xmlcandidate[xmlstart:xmlsize].replace("\\n","\n");
            #Parse the XML
            root=ET.fromstring(xmlrecord)
            for timeinfo in getmoduletimes(root,False):
                if (min_utime == -1) or (timeinfo[1] < min_utime):
                    min_utime = timeinfo[1]
xmlstream.close()

print "Investigation started at :", min_utime

print "Converting input file to output file"
metano=0;
#Open an output file for writing our results (compressed using gzip)
jsonstream = os.popen("/bin/gzip > " + outfile,"w")
#Open our input file once more, using zcat for decompression.
xmlstream = os.popen("/bin/zcat " + infile,"r")
for xmlcandidate in xmlstream:
        #The evidence xml is contained in COPY lines in the mysql ascii dump, check if we have such a line.
        xmlstart = xmlcandidate.find("<evidence ")
        if xmlstart > 0:
            #First extract only the xml part from the COPY line, than unescape the newlines to restore XML BLOB.
            xmlsize = xmlcandidate.find("</evidence>") + 11 
            xmlrecord = xmlcandidate[xmlstart:xmlsize].replace("\\n","\n");
            #Parse the XML
            root=ET.fromstring(xmlrecord)
            #Only meta from content holding entities is relevant for our research
            if "storeref" in root.attrib:
                #We need a unique number for our specific evidence.
                metano = metano + 1
                #Create a result object for this XML CLOB.
                resultobject = {}
                #Store some evidence level meta data
                resultobject["a_store"]=root.attrib["storeref"]
                resultobject["a_meta"]=metano
                #Initialize the size to zero.
                resultobject["a_size"]=0
                #Start with an empty list of jobs.
                resultobject["jobs"] = [];
                for timeinfo in getmoduletimes(root,True):
                     #Create a 'job' output object for this job.
                     thisjob = {}
                     #Use relative timestamps in seconds since the first starttime in this investigation.
                     thisjob["module"] = timeinfo[0]
                     thisjob["time_start"] = timeinfo[1] - min_utime;
                     thisjob["time_stop"] = timeinfo[2] - min_utime;
                     if timeinfo[3] != None:
                         resultobject["a_size"] = timeinfo[3]
                     #Append the new job meta to the list of jobs.
                     resultobject["jobs"].append(thisjob)
                #We are only interested in non-zero-sized evidence.
                #if resultobject["a_size"] > 0:
                #Write the result to the output file.
                jsonstream.write(json.dumps(resultobject))
                jsonstream.write("\n")
xmlstream.close()
jsonstream.close()
