#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#This is a simple example script that embeds four fake workers.
#Normaly each worker will run in its own process with its own script or program.
#First we import the minimal python API
from mattock.api import MountPoint
import sys
#The standard place for our MattockFS mountpoint in the initial release.
mp=MountPoint("/var/mattock/mnt/0")
#Record the starting situation.
fadvise_start=mp.fadvise_status()
worker_count_start={}
anycast_status_start={}
for actorname in ["kickstart","har","bar","baz"]:
  worker_count_start[actorname] = mp.worker_count(actorname)
  anycast_status_start[actorname] = mp.anycast_status(actorname)
#Look at the archive as a whole
whole=mp.full_archive()
#If there is data in the archive, we test opportunistic hashing.
if whole.as_entity().totalsize > 8000:
  print "======== TESTING frozen/ ==========="
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
print "Initial actor worker count kickstart     :",mp.worker_count("kickstart")
context=mp.register_worker("kickstart","K")
print "After kickstart actor worker registration:",mp.worker_count("kickstart")
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
print "Submitting child carvpath to har"
#Fetching fadvise status for har for reference
pre_status=mp.anycast_status("har")
kickstartjob.childsubmit(frozenmutable,"har","t1:l11","x-mattock/harhar","har")
print "Marking parent job as done"
kickstartjob.done()
print "Fetching global fadvise status:"
print " * old  :", fadvise_start
print " * new  :", mp.fadvise_status()
#The child entity has been submitted to the har actor now, lets check the anycast status for that actor.
print "Checking anycast status for har actor"
print " * old anycast status = ",pre_status
print " * new anycast status = ",mp.anycast_status("har")
#
#From now, we pretend we are a har worker
print "Processing the generated job as har"
context=mp.register_worker("har")
#To allow load-balancing, we can set some metrics on the actor.
context.actor_set_weight(7)
context.actor_set_overflow(3)
#Lets poll the job we just submitted when we were kickstart.
harjob=context.poll_job()
if harjob == None:
  print "ERROR, polling the har returned None"
else:
  print "OK; Fetched job, there should be an opportunistic hash over the sparse data!"
  #Get the path of our job data.
  print " * carvpath      = "+harjob.carvpath.as_path()
  #If all data was accessed, the opportunistic hash should be there.
  print " * opportunistic_hash    =",harjob.carvpath.opportunistic_hash()
  #We can pick a subchunk of our input data and submit it as child data. No questions asked.
  print "Submit sub-carvpath 123+1000 as child entity to bar"
  harjob.childsubmit("123+1000","bar","t9:l4","x-mattock/silly-sparse","sparse")
  #We are not done yet with our input data, we forward it to an other actor.
  print "Forward parent entity to baz" 
  harjob.forward("baz","t18:l6")
  #
  #
  #
  #Now we become a bar worker and process the subchunk entity.
  print "Doing nothing as bar"
  context=mp.register_worker("bar")
  barjob = context.poll_job()
  if barjob == None:
    print "ERROR, polling the bar context returned None"
  else:
    print " * routing_info : ", barjob.router_state
    barjob.done()
    print
    #
    #
    #We become a baz worker and process the written-to entity.
    print "Doing nothing as baz"
    context=mp.register_worker("baz")

    bazjob = context.poll_job()
    if bazjob == None:
      print "ERROR, polling the baz returned None"
    else:
      print " * routing_info : ", bazjob.router_state
      bazjob.done()
      context=None
      print "Done"
      fadvise_end=mp.fadvise_status()
      worker_count_end={}
      anycast_status_end={}
      for actorname in ["kickstart","har","bar","baz"]:
        worker_count_end[actorname] = mp.worker_count(actorname)
        anycast_status_end[actorname] = mp.anycast_status(actorname)
      print "Comparing start fadvise to end fadvise state"
      print fadvise_start
      print fadvise_end
      print "Comparing worker count start and end:"
      print worker_count_start
      print worker_count_end
      print "Comparing anycast state start and end:"
      print anycast_status_start
      print anycast_status_end
