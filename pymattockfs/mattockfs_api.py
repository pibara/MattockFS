#!/usr/bin/python
import xattr
import os
import re
from time import sleep

class CarvPathFile:
  def __init__(self,mp,cp):
    self.mp=mp
    self.carvpath=cp
    self.xa=xattr.xattr(mp + "/" + self.carvpath)
  def path_state(self):
    print "fetch path_state from",self.carvpath
    st=self.xa["user.path_state"].split(";")
    return {"hash" : st[0],
            "hash_offset" : int(st[1])}
  def throttle_info(self):
    print "Fetch throttle_info from",self.carvpath
    st=self.xa["user.throttle_info"].split(";")
    return {"normal" : int(st[0]), "dontneed" : int(st[1])}  



class Job:
  def __init__(self,mp,job_ctl):
    self.mp=mp
    self.ctl=xattr.xattr(job_ctl)
    print "Fetch routing_info from",job_ctl 
    self.router_state=self.ctl["user.routing_info"].split(";")[1]
    print "Fetch job_carvpath from",job_ctl
    self.carvpath=CarvPathFile(mp,self.ctl["user.job_carvpath"])
    self.newdata=None
    self.done=False
  def __del__(self):
    self.forward("","")
  def childdata(self,datasize):
    if self.done == False:
      print "Setting create_mutable to ",datasize
      self.ctl["user.create_mutable"]=str(datasize)
      print "Fetching mutable"
      self.newdata=self.mp + "/" +self.ctl["user.mutable"]
      return self.newdata
    return None
  def childsubmit(self,carvpath,nextmodule,routerstate,mimetype,extension):
    if self.done == False:
      print "Setting submit_child to ",carvpath,nextmodule,"...."
      self.ctl["user.submit_child"] = carvpath + ";" + nextmodule + ";" +  routerstate + ";" + mimetype + ";" + extension
      self.newdata=None
  def done(self):
    self.forward(";")
  def forward(self,nextmodule,routerstate):
    if self.done == False:
      print "Setting routing_info to",nextmodule
      self.ctl["user.routing_info"] = nextmodule + ";" + routerstate
      self.done = True

class Context:
  def __init__(self,mountpoint,module,initial_sort_policy=None):
    self.selectre = re.compile(r'^[SVDWC]{1,5}$')
    self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')    
    self.mountpoint=mountpoint
    self.main_ctl=xattr.xattr(self.mountpoint + "/mattockfs.ctl")
    self.module_ctl=xattr.xattr(self.mountpoint + "/module/" + module + ".ctl")
    self.instance_ctl=None
    print "Fetching register_instance"
    path=self.module_ctl["user.register_instance"]
    self.instance_ctl = xattr.xattr(self.mountpoint + "/" + path)
    if not initial_sort_policy == None:
      self.set_sort_policy(initial_sort_policy)
      ok=bool(self.sortre.search(initial_sort_policy))
      if ok:
        print "Setting sort_pollicy"
        self.instance_ctl["user.sort_policy"] = initial_sort_policy
      else:
        raise RuntimeError("Invalid sort policy string")      
  def __del__(self):
    if self.instance_ctl != None:
      print "Setting unregister"
      self.instance_ctl["user.unregister"]="1"
  def set_sort_policy(self,policy):
    ok=bool(self.sortre.search(policy))
    if ok:
      print "Setting sort_policy"
      self.instance_ctl["user.sort_policy"] = policy
    else:
      raise RuntimeError("Invalid sort policy string")
  def set_select_policy(self,pollicy):
    ok=bool(self.selectre.search(policy))
    if ok:
      print "Setting select_policy"
      self.instance_ctl["user.select_policy"] = policy
    else:
      raise RuntimeError("Invalid select policy string")
  def poll_job(self):
    print "Fetching accept_job"
    job=self.instance_ctl["user.accept_job"]
    if job == "":
      return None
    return Job(self.mountpoint,self.mountpoint + "/" + job)
  def get_job(self):
    while True:
      job=self.poll_job()
      if job == None:
        sleep(0.05)
      else:
        yield job
  def fs_throttle_info(self):
    print "Fetching throttle_info from mattockfs.ctl"
    st=self.main_ctl["user.throttle_info"].split(";")
    return {"normal": int(st[0]), "dontneed" : int(st[1])}
  def module_set_weight(self,weight):
    print "Setting weight"
    self.module_ctl["user.weight"] = str(weight)
  def module_set_overflow(self,weight):
    print "Setting overflow"
    self.module_ctl["user.overflow"] = str(overflow)
  def module_instance_count(self):
    print "Fetching instance_count"
    return int(self.module_ctl["user.instance_count"])
  #WARNING, will mess with any running module by revoking its instance handle and anything below that.
  def module_reset(self):
    print "Setting reset"
    self.module_ctl["user.reset"]="1"
  def anycast_throttle_info(self,peermodule):
    print "Fetching throttle_info from module ctl"
    st=self.module_ctl["user.throttle_info"].split(";")
    return {"set_size" : int(st[0]), "set_volume" : int(st[1])}

if __name__ == '__main__':
  mountpoint="/home/larissa/src/mattock-dissertation/pymattockfs/mnt"
  #mountpoint="/export/home/rob/dissertation/mattock-dissertation/pymattockfs/mnt"
  context=Context(mountpoint,"kickstart","K")
  print context.fs_throttle_info()
  job=context.poll_job()
  print job.carvpath.carvpath
  print job.carvpath.path_state()
  print job.carvpath.throttle_info()
  print context.module_instance_count()
  kickfile=job.childdata(1234567)
  with open(kickfile,"wb") as f:
    f.seek(0,0)
    f.write("harhar")
    f.seek(0,1234560)
    f.write("HARHAR")
  job.childsubmit("harmodule","t1:l11","x-mattock/harhar","har")
  job.done()
  print context.anycast_throttle_info("harmodule")
  context2=Context(mountpoint,"harmodule")
  context2.module_set_weight(7)
  context2.module_set_overflow(3)
  job2=context2.poll_job()
  print job2.carvpath.carvpath
  print job2.carvpath.path_state()
  print job2.carvpath.throttle_info()
  job2.childsubmit("123+1000","barmod","t9:l4","x-mattock/silly-sparse","sparse")
  job.forward("bazmod","t18:l6")
