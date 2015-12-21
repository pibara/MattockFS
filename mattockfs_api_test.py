#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#This is a simple example script that embeds four fake modules.
#Normaly each module will run in its own process with its own script or program.
#First we import the minimal python API
from mattock.api import MountPoint
import sys
#The standard place for our MattockFS mountpoint in the initial release.
mp=MountPoint("/var/mattock/mnt/0")
#Record the starting situation.
fadvise_start=mp.fadvise_status()
module_instance_count_start={}
anycast_status_start={}
for modulename in ["kickstart","harmodule","barmod","bazmod"]:
  module_instance_count_start[modulename] = mp.module_instance_count(modulename)
  anycast_status_start[modulename] = mp.anycast_status(modulename)
#Look at the archive as a whole
whole=mp.full_archive()
#If there is data in the archive, we test opportunistic hashing.
if whole.as_entity().totalsize > 8000:
  print "======== TESTING data/ ==========="
  sub1=whole["500+1800_3000+1000.gif"]
  sub2=whole["0+8000.dat"]
  sub3=whole["500+1800_S19000_3000+1000.gif"]
  print "Testing full path resolution for sub path"
  fp=mp.full_path(sub3.as_entity())
  print fp
  #Open all three files
  f1=open(sub1.as_path(),"r")
  f2=open(sub2.as_path(),"r")
  f3=open(sub3.as_path(),"r")
  #Read only from file two
  a=f2.read()
  #If everything is iree, both files should have been hashed now.
  print "We read only one file but the other 2 should have been hashed opportunistically also"
  print sub1.opportunistic_hash()  
  print sub2.opportunistic_hash()
  print sub3.opportunistic_hash()
  sub4=whole["7000+3512.dat"]
  print "Testing fadvise on non open partially overlapping entity"
  print sub4.fadvise_status()
  print "Testing global fadvise"
  print "start:", fadvise_start
  print "openf:", mp.fadvise_status()
  f1.close()
  f2.close()
  f3.close()
  print "closf:", mp.fadvise_status()
else:
  print "Skipping carvpath test, to little data in the archive."
print "======= Testing kickstarting API walkthrough  ========="
print "Initial module count kickstart     :",mp.module_instance_count("kickstart")
context=mp.register_instance("kickstart","K")
print "After kickstart module registration:",mp.module_instance_count("kickstart")
kickstartjob=context.poll_job()
print "Job info:"
print " * carvpath      = " + kickstartjob.carvpath.as_path()
print " * router_state  = " + kickstartjob.router_state
print "Creating new mutable entity within job context"
mutabledata=kickstartjob.childdata(1234567)
print " * mutabledata =", mutabledata
print "Writing to mutable file"
with open(mutabledata,"r+") as f:
  f.seek(0)
  f.write("harhar")
  #The file can very well be sparse if we want it to.
  f.seek(1234560)
  f.write("HARHAR12")
#Once we are done writing the data, we freeze it and get a carvpath back.
print "Freezing mutable file"
frozenmutable=kickstartjob.frozen_childdata()
print " * Carvpath =", frozenmutable
print "Submitting child carvpath to harmodule"
#Fetching fadvise status for harmodule for reference
pre_status=mp.anycast_status("harmodule")
kickstartjob.childsubmit(frozenmutable,"harmodule","t1:l11","x-mattock/harhar","har")
print "Marking parent job as done"
kickstartjob.done()
print "Fetching global fadvise status:"
print " * old  :", fadvise_start
print " * new  :", mp.fadvise_status()
#The child entity has been submitted to the harmodule now, lets check the anycast status for that module.
print "Checking anycast status for harmodule"
print " * old anycast status = ",pre_status
print " * new anycast status = ",mp.anycast_status("harmodule")
#
#From now, we pretend we are the harmodule
print "Processing the generated job as harmodule"
context=mp.register_instance("harmodule")
#To allow load-balancing, we can set some metrics on the module.
context.module_set_weight(7)
context.module_set_overflow(3)
#Lets poll the job we just submitted when we were kickstart.
harjob=context.poll_job()
if harjob == None:
  print "ERROR, polling the harmodule returned None"
else:
  print "OK; Fetched job, there should be an opportunistic hash over the sparse data!"
  #Get the path of our job data.
  print " * carvpath      = "+harjob.carvpath.as_path()
  #If all data was accessed, the opportunistic hash should be there.
  print " * opportunistic_hash    =",harjob.carvpath.opportunistic_hash()
  #We can pick a subchunk of our input data and submit it as child data. No questions asked.
  print "Submit sub-carvpath 123+1000 as child entity to barmod"
  harjob.childsubmit("123+1000","barmod","t9:l4","x-mattock/silly-sparse","sparse")
  #We are not done yet with our input data, we forward it to an other module.
  print "Forward parent entity to bazmod" 
  harjob.forward("bazmod","t18:l6")
  #
  #
  #
  #Now we become the barmodule and process the subchunk entity.
  print "Doing nothing as barmod"
  context=mp.register_instance("barmod")
  barjob = context.poll_job()
  if barjob == None:
    print "ERROR, polling the barmod returned None"
  else:
    print " * routing_info : ", barjob.router_state
    barjob.done()
    print
    #
    #
    #We become the bazmod module and process the written-to entity.
    print "Doing nothing as bazmod"
    context=mp.register_instance("bazmod")

    bazjob = context.poll_job()
    if bazjob == None:
      print "ERROR, polling the bazmod returned None"
    else:
      print " * routing_info : ", bazjob.router_state
      bazjob.done()
      context=None
      print "Done"
      fadvise_end=mp.fadvise_status()
      module_instance_count_end={}
      anycast_status_end={}
      for modulename in ["kickstart","harmodule","barmod","bazmod"]:
        module_instance_count_end[modulename] = mp.module_instance_count(modulename)
        anycast_status_end[modulename] = mp.anycast_status(modulename)
      print "Comparing start fadvise to end fadvise state"
      print fadvise_start
      print fadvise_end
      print "Comparing instance count start and end:"
      print module_instance_count_start
      print module_instance_count_end
      print "Comparing anycast state start and end:"
      print anycast_status_start
      print anycast_status_end
