#!/usr/bin/python
import mattock
import os
import json
def get_instance_count():
    try:
        json_data = open("/etc/mattockfs.json").read()
        data = json.loads(json_data)
        return data["instance_count"]
    except:
        print "No config file found, running two default instances"
        return 2

for instance in range(0,get_instance_count()):
    command = "fusermount -u /var/mattock/mnt/" + str(instance)
    os.system(command)
