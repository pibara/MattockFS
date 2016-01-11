#!/usr/bin/python
# Copyright (c) 2015, Rob J Meijer.
# Copyright (c) 2015, University College Dublin
# All rights reserved.
#
# This is a simple example script that embeds four fake workers.
# Normaly each worker will run in its own process with its own script or
# program.
# First we import the minimal python API
from mattock.api import MountPoint
import sys


def test_bogus_path(base):
    for subdir in ("job", "mutable", "worker"):
        ext = "ctl"
        if subdir == "mutable":
            ext = "dat"
        path = (base + "/" + subdir + "/C21de91cabe9f201cebe3d39834eb8c0b9" +
                "fcd12688082200658ba46a02b66147b." + ext)
        try:
            f = open(path, "r")
            print "FAIL: Bogus file not suposed to exist ;", path
            f.close()
        except:
            pass


def test_add_data_to_job(job):
    mutable = job.childdata(1000000)
    with open(mutable, "r+") as f:
        f.seek(0)
        f.write("harhar")
        # The file can very well be sparse if we want it to.
        f.seek(500000)
        f.write("HARHAR")
    return


def test_add_poisoned_data_to_job(job):
    mutable = job.childdata(1000000)
    with open(mutable, "r+") as f:
        f.seek(0)
        f.write("harhar")
        # The file can very well be sparse if we want it to.
        f.seek(500000)
        f.write("HARHAR")
        # Poison the hashing
        f.seek(200000)
        f.write("poison")
    return


def test_anycast_coverage(mp):
    nomodule = mp.register_worker("bogusmodule")
    nojob = nomodule.poll_job()
    if nojob is not None:
        print "FAIL: polling bogusmodule should return None"
    mp.actor_reset("kickstart")
    kickstart = mp.register_worker("kickstart", "K")
    if mp.worker_count("kickstart") != 1:
        print("FAIL: Kickstart worker count should be '1' ; it is ",
              mp.worker_count("kickstart"))
    kickstartjob = kickstart.poll_job()
    if kickstartjob is None:
        print ("FAIL: polling kickstart should always yield a job," +
               "None returned")
        return
    if kickstartjob.frozen_childdata() is not None:
        print ("FAIL: shouldn't be able to freeze child data that was never" +
               "allocated.")
    test_add_data_to_job(kickstartjob)
    test_add_poisoned_data_to_job(kickstartjob)
    test_add_data_to_job(kickstartjob)
    path = kickstartjob.frozen_childdata()
    kickstartjob.childsubmit(path, "foo", "routerstate123",
                             "x-mattock/testdata", "data")
    test_add_data_to_job(kickstartjob)
    path = kickstartjob.frozen_childdata()
    kickstartjob.childsubmit(path, "foo", "routerstate234",
                             "x-mattock/testdata", "data")
    status = mp.anycast_status("foo")
    if status["set_size"] != 2:
        print "FAIL: unexpected set size", status["set_size"], "expected 2"
    else:
        if status["set_volume"] != 2000000:
            print("FAIL: unexpected set volume", status["set_volume"],
                  "expected 20000")
    foo = mp.register_worker("foo", "RDOWHS")
    foo.actor_set_overflow(0)
    foo.actor_set_weight(1000)
    lbjob = foo.poll_job()
    mp.actor_reset("foo")
    loadbalance = mp.register_worker("loadbalance")
    lbjob = loadbalance.poll_job()
    if lbjob is None:
        print "FAIL: Problem fetching job with loadbalance"
        return
    else:
        lbjob.forward("bar", "routerstate789")
    if kickstartjob.frozen_childdata() is None:
        print "FAIL: freezing a mutable should yield a non-None value."
    try:
        bogus1 = mp.register_worker("a")
    except:
        pass
    try:
        bogus2 = mp.register_worker("#!`+apn^")
    except:
        pass
    try:
        bogus3 = mp.register_worker("abcdefghijklmnopqrstuvwxyz0123456789" +
                                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    except:
        pass


def test_carvpath(mp):
    # Look at the archive as a whole
    whole = mp.full_archive()
    # If there is data in the archive, we test opportunistic hashing.
    if whole.as_entity().totalsize > 8000:
        sub1 = whole["500+1800_3000+1000.gif"]
        sub2 = whole["0+8000.dat"]
        sub3 = whole["500+1800_S19000_3000+1000.gif"]
        fp = mp.full_path(sub3.as_entity())
        if fp is None:
            print "ERR: No full path"
        # Open all three files
        f1 = open(sub1.as_path(), "r")
        f2 = open(sub2.as_path(), "r")
        f3 = open(sub3.as_path(), "r")
        # Read only from file two
        a = f2.read()
        # If everything is iree, both files should have been hashed now.
        print sub1.opportunistic_hash()
        print sub2.opportunistic_hash()
        print sub3.opportunistic_hash()
        sub4 = whole["7000+3512.dat"]
        print sub4.fadvise_status()
        print "openf:", mp.fadvise_status()
        f1.close()
        f2.close()
        f3.close()
        print "closf:", mp.fadvise_status()
        str1 = ("0+100_101+100_202+100_303+100_404+100_505+100_" +
                "606+100_707+100_808+100_909+100_1010+100_1111+100" +
                "_1212+100_1313+100_1414+100_1515+100_1616+100_1717" +
                "+100_1818+100_1919+100_2020+100_2121+100_2222+100_" +
                "2323+100_2424+100")
        sub4 = whole[str1]
        print sub4.as_path()
    else:
        print "Skipping carvpath test, to little data in the archive."

# The standard place for our MattockFS mountpoint in the initial release.
mp = MountPoint("/var/mattock/mnt/0")
test_bogus_path("/var/mattock/mnt/0")
test_anycast_coverage(mp)
# Record the starting situation.
fadvise_start = mp.fadvise_status()
worker_count_start = {}
anycast_status_start = {}
for actorname in ["kickstart", "har", "bar", "baz"]:
    worker_count_start[actorname] = mp.worker_count(actorname)
    anycast_status_start[actorname] = mp.anycast_status(actorname)
test_carvpath(mp)

# print "======= Testing kickstarting API walkthrough  ========="
# print "Initial actor worker count kickstart     :",mp.worker_count(
#         "kickstart")
# context=mp.register_worker("kickstart","K")
# print "After kickstart actor worker registration:",mp.worker_count(
#         "kickstart")
# kickstartjob=context.poll_job()
# print "Job info:"
# print " * carvpath      = " + kickstartjob.carvpath.as_path()
# print " * router_state  = " + kickstartjob.router_state
# print "Creating new mutable entities within job context"
# for time in range(0,3):
#  mutabledata=kickstartjob.childdata(1234567)
#  print " * mutabledata =", mutabledata
#  print "Writing to mutable file"
#  with open(mutabledata,"r+") as f:
#    f.seek(0)
#    f.write("harhar")
#    #The file can very well be sparse if we want it to.
#    f.seek(1234500)
#    f.write("HARHAR")
#    if time == 2:
#      f.seek(1000000)
#      f.write("poison")
# Once we are done writing the data, we freeze it and get a carvpath back.
#  print "Freezing mutable file"
#  frozenmutable=kickstartjob.frozen_childdata()
#  print " * Carvpath =", frozenmutable
#  print "Submitting child carvpath to har"
# Fetching fadvise status for har for reference
#  pre_status=mp.anycast_status("har")
#  kickstartjob.childsubmit(frozenmutable,"har","t1:l11","x-mattock/harhar",
#                                       "har")
# print "Marking parent job as done"
# kickstartjob.done()
# print "Fetching global fadvise status:"
# print " * old  :", fadvise_start
# print " * new  :", mp.fadvise_status()
# The child entity has been submitted to the har actor now, lets check the
#     anycast status for that actor.
# print "Checking anycast status for har actor"
# print " * old anycast status = ",pre_status
# print " * new anycast status = ",mp.anycast_status("har")
#
# From now, we pretend we are a har worker
# print "Processing the generated job as har"
# context=mp.register_worker("har")
# To allow load-balancing, we can set some metrics on the actor.
# context.actor_set_weight(7)
# context.actor_set_overflow(3)
# Lets poll the job we just submitted when we were kickstart.
# for time in range(0,3):
#  harjob=context.poll_job()
#  if harjob == None:
#    print "ERROR, polling the har returned None"
#  else:
#    print "OK; Fetched job, there should be an opportunistic hash over
#               the sparse data!"
# Get the path of our job data.
#    print " * carvpath      = "+harjob.carvpath.as_path()
#    print " * hash   = ",harjob.carvpath.opportunistic_hash()
#    print " * fadvise= ",harjob.carvpath.fadvise_status()
# If all data was accessed, the opportunistic hash should be there.
#    print " * opportunistic_hash    =",harjob.carvpath.opportunistic_hash()
# We can pick a subchunk of our input data and submit it as child data.
#          No questions asked.
#    print "Submit sub-carvpath 123+1000 as child entity to bar"
#    harjob.childsubmit("123+1000_S9000_234+1000_S9000_345+9000_S99000_456+
# 9000_S999000_567+9000_678+9000_S1000000_789+9000_S2000000_1234+8000_S3000000
# _2345+8000_S4000000_3456+8000_S5000000_4567+8000_S6000000_5678+8000_S7000000
# _6789+8000_S8000000","bar","t9:l4","x-mattock/silly-sparse","sparse")
# We are not done yet with our input data, we forward it to an other actor.
#    print "Forward parent entity to baz"
#    harjob.forward("baz","t18:l6")
# Now we become a bar worker and process the subchunk entity.
# print "Doing nothing as bar"
# context=mp.register_worker("bar")
# for time in range(0,3):
#  barjob = context.poll_job()
#  if barjob == None:
#    print "ERROR, polling the bar context returned None"
#  else:
#    print " * carvpath      = "+barjob.carvpath.as_path()
#    print " * routing_info : ", barjob.router_state
#    print " * hash   = ",barjob.carvpath.opportunistic_hash()
#    print " * fadvise= ",barjob.carvpath.fadvise_status()
#    barjob.done()
#    print
# We become a baz worker and process the written-to entity.
# print "Doing nothing as baz"
# context=mp.register_worker("baz")
# for time in range(0,3):
#    bazjob = context.poll_job()
#    if bazjob == None:
#      print "ERROR, polling the baz returned None"
#    else:
#      print " * routing_info : ", bazjob.router_state
#      bazjob.done()
# context=None
# print "Done"
# fadvise_end=mp.fadvise_status()
# worker_count_end={}
# anycast_status_end={}
# for actorname in ["kickstart","har","bar","baz"]:
#  worker_count_end[actorname] = mp.worker_count(actorname)
#  anycast_status_end[actorname] = mp.anycast_status(actorname)
# print "Comparing start fadvise to end fadvise state"
# print fadvise_start
# print fadvise_end
# print "Comparing worker count start and end:"
# print worker_count_start
# print worker_count_end
# print "Comparing anycast state start and end:"
# print anycast_status_start
# print anycast_status_end
