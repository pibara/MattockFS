#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#This is a simple example script that embeds four fake modules.
#Normaly each module will run in its own process with its own script or program.
#First we import the minimal python API
from mattock.api import Context

#The standard place for our MattockFS mountpoint in the initial release.
mountpoint="/var/mattock/mnt/0"
#First we act like a kickstart and enter some data.
print "Regestering kickstart module and setting \"K\" sort policy"
kickstartcontext=Context(mountpoint,"kickstart","K")
#In this example we don't act on it, but this info may be used for throttling new data input.
print "Fetching global fadvise status:"
print " * ", kickstartcontext.fadvise_status()
#We are running in "K" mode so we cn grab a job out of thin air here
print "Accepting our first job"
kickstartjob=kickstartcontext.poll_job()
print " * carvpath      = "+mountpoint+"/"+kickstartjob.carvpath.carvpath
#There will be nothing here now, but this is where an opportunistic hash may arise.
print " * opportunistic_hash    = ",kickstartjob.carvpath.opportunistic_hash()
#Not of much use now as this one is zero size, but this will tell if any part of the carvpath
#is marked as dontneed.
print " * fadvise_status = ",kickstartjob.carvpath.fadvise_status()
#There might be more than one kickstart currently alive, lets count how many there are.
print " * instance_count= ",kickstartcontext.module_instance_count()
#Allocate a piece of mutable data to put our kickstart data in. The pseudo file
#is fixed size and writable untill frozen. The file can NOT be truncated so open
#it appropriately!
print "Creating new mutable entity within job context"
mutabledata=kickstartjob.childdata(1234567)
print " * mutabledata =", mutabledata
print "Writing to mutable file"
with open(mutabledata,"r+") as f:
  f.seek(0)
  f.write("harhar")
  #The file can very well be sparse if we want it to.
  f.seek(1234560)
  f.write("HARHAR")
#Once we are done writing the data, we freeze it and get a carvpath back.
print "Freezing mutable file"
frozenmutable=kickstartjob.frozen_childdata()
print " * Carvpath =", frozenmutable
#The carvpath of the frozen entity can be submitted as child of the job we are processing.
#We need to specify the next module that is to process this entity. We may specify a short 
#router-state string for maintaining router state between modules processing the same entity,
#and we specify the content mime-type and prefered file-extension.
print "Submitting child carvpath to harmodule"
kickstartjob.childsubmit(frozenmutable,"harmodule","t1:l11","x-mattock/harhar","har")
#Nothing more to do with our parent job. We mark it as done.
print "Marking parent job as done"
kickstartjob.done()
print "Fetching global fadvise status:"
print " * ", kickstartcontext.fadvise_status()
#The child entity has been submitted to the harmodule now, lets check the anycast status for that module.
print "Checking anycast status for harmodule"
print " * throttle info = ",kickstartcontext.anycast_status("harmodule")
#
#
#From now, we pretend we are the harmodule
print "Register as harmodule"
harcontext=Context(mountpoint,"harmodule")
#To allow load-balancing, we can set some metrics on the module.
print "Setting weight and overflow for harmodule"
harcontext.module_set_weight(7)
harcontext.module_set_overflow(3)
#Lets poll the job we just submitted when we were kickstart.
print "There should be one job, poll it"
harjob=harcontext.poll_job()
if harjob == None:
  print "ERROR, polling the harmodule returned None"
else:
  print "Fetched job"
  #Get the path of our job data.
  print " * carvpath      = "+mountpoint + "/" + harjob.carvpath.carvpath
  #If all data was accessed, the opportunistic hash should be there.
  print " * opportunistic_hash    =",harjob.carvpath.opportunistic_hash()
  #We can pick a subchunk of our input data and submit it as child data. No questions asked.
  print "Submit sub-carvpath 123+1000 as child entity to barmod"
  harjob.childsubmit("123+1000","barmod","t9:l4","x-mattock/silly-sparse","sparse")
  #We are not done yet with our input data, we forward it to an other module.
  print "Forward parent entity to bazmod" 
  harjob.forward("bazmod","t18:l6")
  print 
  print "Fetching global fadvise status:"
  print " * ", kickstartcontext.fadvise_status()
  #
  #
  #
  #Now we become the barmodule and process the subchunk entity.
  print "Doing nothing as barmod"
  barcontext=Context(mountpoint,"barmod")
  barjob = barcontext.poll_job()
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
    bazcontext=Context(mountpoint,"bazmod")
    bazjob = bazcontext.poll_job()
    if bazjob == None:
      print "ERROR, polling the bazmod returned None"
    else:
      bazjob.done()
print "Fetching global fadvise status:"
print " * ", kickstartcontext.fadvise_status()
print "Done"
