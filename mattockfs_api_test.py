#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#Temporary script for walking to some of the base API on top of MattockFS.
#I shall be converting this script into a set of integration test runs soon.
from mattock.api import Context

#The standard place for our MattockFS mountpoint in the initial release.
mountpoint="/var/mattock/mnt/0"
print "Regestering kickstart module and setting \"K\" sort policy"
kickstartcontext=Context(mountpoint,"kickstart","K")
print "Full archive path:"+mountpoint+"/"+kickstartcontext.full_archive_path()
print "Fetching global fadvise status:"
print " * ", kickstartcontext.fadvise_status()
print "Accepting our first job"
kickstartjob=kickstartcontext.poll_job()
print " * carvpath      = "+mountpoint+"/"+kickstartjob.carvpath.carvpath
print " * opportunistic_hash    = ",kickstartjob.carvpath.opportunistic_hash()
print " * fadvise_status = ",kickstartjob.carvpath.fadvise_status()
print " * instance_count= ",kickstartcontext.module_instance_count()
print "Creating new mutable entity within job context"
mutabledata=kickstartjob.childdata(1234567)
print " * mutabledata =", mutabledata
print "Writing to mutable file"
with open(mutabledata,"r+") as f:
  f.seek(0)
  f.write("harhar")
  f.seek(1234560)
  f.write("HARHAR")
print "Freezing mutable file"
frozenmutable=kickstartjob.frozen_childdata()
print " * Carvpath =", frozenmutable
print "Submitting child carvpath to harmodule"
kickstartjob.childsubmit(frozenmutable,"harmodule","t1:l11","x-mattock/harhar","har")
print "Marking parent job as done"
kickstartjob.done()
#
#
#
print "Checking anycast status for harmodule"
print " * throttle info = ",kickstartcontext.anycast_status("harmodule")
#
#
#
print "Register as harmodule"
harcontext=Context(mountpoint,"harmodule")
print "Setting weight and overflow for harmodule"
harcontext.module_set_weight(7)
harcontext.module_set_overflow(3)
print "There should be one job, poll it"
harjob=harcontext.poll_job()
if harjob == None:
  print "ERROR, polling the harmodule returned None"
else:
  print "Fetched job"
  print " * carvpath      = "+mountpoint + "/" + harjob.carvpath.carvpath
  print " * opportunistic_hash    =",harjob.carvpath.opportunistic_hash()
  print " * fadvise_status =", harjob.carvpath.fadvise_status()
  print "Submit sub-carvpath 123+1000 as child entity to barmod"
  harjob.childsubmit("123+1000","barmod","t9:l4","x-mattock/silly-sparse","sparse")
  print "Forward parent entity to bazmod" 
  harjob.forward("bazmod","t18:l6")
  print 
  #
  #
  #
  #
  print "Doing nothing as barmod"
  barcontext=Context(mountpoint,"barmod")
  barjob = barcontext.poll_job()
  print " * routing_info : ", barjob.router_state
  barjob.done
  print
  #
  #
  #
  # 
  print "Doing nothing as bazmod"
  bazcontext=Context(mountpoint,"bazmod")
  bazjob = bazcontext.poll_job()
  bazjob.done
print "Done"
