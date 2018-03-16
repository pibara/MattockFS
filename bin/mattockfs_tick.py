#!/usr/bin/python
import xattr
import time
import json
instance_count = 1
with open("/etc/mattockfs.json","r") as configfile:
    conf = json.loads(configfile.read())
    if "instance_count" in conf:
        instance_count = conf["instance_count"]
while True:
    for inum in range(0,instance_count):
        try:
            path = "/var/mattock/mnt/" + str(inum) + "/mattockfs.ctl"
            main_ctl = xattr.xattr(path)
            dummy = main_ctl["user.tick"]
        except:
            pass
    time.sleep(60)
    
