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
    for boguspath in ("job/foo/bar.dat", "carvpath/foo/bar/baz.gif"):
        path = (base + "/" + boguspath)
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
    kickstartjob.childsubmit(carvpath=path,
                             nextactor="foo",
                             routerstate="routerstate123",
                             mimetype="x-mattock/testdata",
                             extension="data")
    test_add_data_to_job(kickstartjob)
    path = kickstartjob.frozen_childdata()
    kickstartjob.childsubmit(carvpath=path,
                             nextactor="foo",
                             routerstate="routerstate234",
                             mimetype="x-mattock/testdata",
                             extension="data")
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
    lbjob.done()
    # mp.actor_reset("foo")
    loadbalance = mp.register_worker("loadbalance")
    loadbalance.set_actor_select_policy("VWC")
    lbjob = loadbalance.poll_job()
    if lbjob is None:
        print "FAIL: Problem fetching job with loadbalance"
        return
    else:
        lbjob.forward("bar", "routerstate789")
        do_bar(mp, times=1)
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
        f1 = open(sub1.as_file_path(), "r")
        f2 = open(sub2.as_file_path(), "r")
        f3 = open(sub3.as_file_path(), "r")
        # Read only from file two
        a = f2.read()
        # If everything is iree, both files should have been hashed now.
        if len(sub1.opportunistic_hash()["hash"]) != 64:
            print "Opportunistic hashing error 1: ", sub1.opportunistic_hash()
        if len(sub2.opportunistic_hash()["hash"]) != 64:
            print "Opportunistic hashing error 2: ", sub2.opportunistic_hash()
        if len(sub3.opportunistic_hash()["hash"]) != 64:
            print "Opportunistic hashing error 3: ", sub3.opportunistic_hash()
        sub4 = whole["7000+3512.dat"]
        if sub4.fadvise_status()["normal"] != 1000:
            print ("Issue with expected fadvise overlap of 1000 " +
                   sub4.fadvise_status())
        if sub4.fadvise_status()["dontneed"] != 2512:
            print ("Issue with expected fadvise non-overlap region of 2512" +
                   sub4.fadvise_status())
        beforeclose = mp.fadvise_status()
        f1.close()
        f2.close()
        f3.close()
        afterclose = mp.fadvise_status()
        if beforeclose["normal"] - afterclose["normal"] != 8000:
            print "Unexpected difference in normal", beforeclose, afterclose
        if afterclose["dontneed"] - beforeclose["dontneed"] != 8000:
            print "Unexpected diference in dontneed", beforeclose, afterclose
        str1 = ("0+100_101+100_202+100_303+100_404+100_505+100_" +
                "606+100_707+100_808+100_909+100_1010+100_1111+100" +
                "_1212+100_1313+100_1414+100_1515+100_1616+100_1717" +
                "+100_1818+100_1919+100_2020+100_2121+100_2222+100_" +
                "2323+100_2424+100.dd")
        sub4 = whole[str1]
        digestpath = sub4.as_file_path()
        if len(digestpath) - digestpath.rfind("/") != 69:
            print "ERROR: unexpected length of digest path: ", digestpath
        try:
            f = open(digestpath, "r")
            f.close()
        except:
            print "ERROR: unable to open ", digestpath
    else:
        print "Skipping carvpath test, to little data in the archive."
    fullsize = mp.full_archive().file_size()
    sizelow = fullsize - 50
    borderpath = str(sizelow) + "+100.dat"
    try:
        bogus2 = mp[borderpath]
        print ("ERROR: a sub file beyond archive size should not exist." +
               borderpath)
    except:
        pass


def do_kickstart(mp):
    beforecount = mp.worker_count("kickstart")
    context = mp.register_worker("kickstart", "K")
    aftercount = mp.worker_count("kickstart")
    if aftercount - beforecount != 1:
        print "ERROR, module count messed up", beforecount, "->", aftercount
    kickstartjob = context.poll_job()
    if kickstartjob is None:
        print "ERROR, kickstart jobs should be creatable out of thin air"
        return
    har_pre_status = mp.anycast_status("har")
    fadvise_pre_status = mp.fadvise_status()
    for time in range(0, 5):
        if time == 2:
            test_add_poisoned_data_to_job(kickstartjob)
        else:
            test_add_data_to_job(kickstartjob)
        frozenmutable = kickstartjob.frozen_childdata()
        kickstartjob.childsubmit(carvpath=frozenmutable,
                                 nextactor="har",
                                 routerstate="t1:l11",
                                 mimetype="x-mattock/harhar",
                                 extension="har")
    kickstartjob.done()
    har_post_status = mp.anycast_status("har")
    fadvise_post_status = mp.fadvise_status()
    if har_post_status["set_size"] - har_pre_status["set_size"] != 5:
        print ("ERROR, wrong set size diff (should be 5): " +
               str(har_pre_status) +
               " :: " +
               str(har_post_status))
    if har_post_status["set_volume"] - har_pre_status["set_volume"] != 5000000:
        print ("ERROR, wrong set volume diff (should be 5000000): " +
               str(har_pre_status) +
               " :: " +
               str(har_post_status))
    if fadvise_pre_status["dontneed"] != fadvise_post_status["dontneed"]:
        print ("ERROR, dontneed should not have changed " +
               str(fadvise_pre_status) +
               " :: " +
               str(fadvise_post_status))
    if fadvise_post_status["normal"] - fadvise_pre_status["normal"] != 5000000:
        print ("ERROR, normal should have grown 5M " +
               str(fadvise_pre_status) +
               " :: " +
               str(fadvise_post_status))


def do_har(mp):
    context = mp.register_worker("har")
    context.actor_set_weight(7)
    context.actor_set_overflow(3)
    ohcount = 0
    for time in range(0, 5):
        harjob = context.poll_job()
        if harjob is None:
            print "ERROR, unable to fetch har job", time
            return
        if len(harjob.carvpath.opportunistic_hash()["hash"]) == 64:
            ohcount += 1
        harjob.childsubmit(carvpath="123+1000_S9000_234+1000_S9000_345+9000_"
                           "S99000_456+9000_S999000_567+9000_678+9000_"
                           "S1000000_789+9000_S2000000_1234+8000_S3000000_"
                           "2345+8000_S4000000_3456+8000_S5000000_4567+8000_"
                           "S6000000_5678+8000_S7000000_6789+8000_S8000000",
                           nextactor="bar",
                           routerstate="t9:l4",
                           mimetype="x-mattock/silly-sparse",
                           extension="sparse")
        harjob.forward("baz", "t18:l6")
    if ohcount != 4:
        print ("ERROR, expected 4 succesfull opportunistic hashes NOT : " +
               str(ohcount))


def do_bar(mp, times=5):
    context = mp.register_worker("bar")
    for time in range(0, times):
        barjob = context.poll_job()
        if barjob is None:
            print "ERROR, polling the bar context", time, "returned None"
            return
        barjob.done()


def do_baz(mp):
    context = mp.register_worker("baz")
    for time in range(0, 5):
        bazjob = context.poll_job()
        if bazjob is None:
            print "ERROR, polling the baz", time, "returned None"
            return
        bazjob.done()


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
do_kickstart(mp)
do_har(mp)
do_bar(mp)
do_baz(mp)
fadvise_end = mp.fadvise_status()
worker_count_end = {}
anycast_status_end = {}
for actorname in ["kickstart", "har", "bar", "baz"]:
    if worker_count_start[actorname] != mp.worker_count(actorname):
        print "ERROR: worker count changed for", actorname
    if (anycast_status_start[actorname]["set_size"] !=
       mp.anycast_status(actorname)["set_size"]):
        print "ERROR: anycast set size changed for", actorname
# print "Comparing start fadvise to end fadvise state"
if fadvise_end["dontneed"] - fadvise_start["dontneed"] != 5000000:
    print "ERROR, unexpexted fadvise changes", fadvise_start, fadvise_end
print "If all tests succeeded, this should be the only output line"
