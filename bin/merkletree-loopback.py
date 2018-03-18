#!/usr/bin/python
import os
import json
from sh import tail
from mattock.api import MountPoint

def get_instance_count():
    try:
        json_data = open("/etc/mattockfs.json").read()
        data = json.loads(json_data)
        return data["instance_count"]
    except:
        print "No config file found, running two default instances"
        return 2

def tail_main(instance):
    mp = MountPoint("/var/mattock/mnt/0")
    context = mp.register_worker("mtloopbck","K")
    tailpath = "/var/mattock/log/" + instance + ".merkletree"
    for line in tail("-f",tailpath,_iter=True):
        kickjob = context.poll_job()
        mutable = kickjob.childdata(len(line))
        with open(mutable, "r+") as f:
            f.seek(0)
            f.write(line)
        merkletree_carvpath = kickjob.frozen_childdata()
        kickjob.childsubmit(carvpath=merkletree_carvpath,
                                    nextactor="blockchainintegrity",
                                    routerstate="",
                                    mimetype="mattock-meta/merkletree",
                                    extension="json")
        kickjob.done()

for instance in range(1,get_instance_count()):
   newpid=os.fork()
   if newpid == 0:
       tail_main(str(instance))
       os._exit(0)
tail_main("0")

