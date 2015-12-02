#!/usr/bin/python
import xattr
import os
from time import sleep

class CarvPathFile:
  def __init__(self,mp,cp):
    self.mp=mp
    self.carvpath=cp
    self xa=xattr.xattr(self.carvpath)
  def path_state(self):

  def throttle_info(self):



class Job:
  def __init__(self,mp,job_ctl):
    self.mp=mp
    self.ctl=xattr.xattr(job_ctl)
    self.router_state=self.ctl["routing_info"].split(";")[1]
    self.carvpath=CarvPathFile(self.ctl["user.job_carvpath"])
    self.newdata=None
  def childdata(self,datasize):
    self.ctl["user.create_mutable_child_entity"]=str(datasize)
    self.newdata=self.mp + "/" +self.ctl["user.create_mutable_child_entity"]
    return self.newdata
  def childcarvpath(self,carvpath):
    self.ctl["user.derive_child_entity"] = carvpath
  def childsubmit(self,nextmodule,routerstate,mimetype,extension):
    self.ctl["user.set_child_submit_info"] = nextmodule + ";" + routerstate + ";" + mimetype + ";" + extension
    self.newdata=None
  def done(self):
    self.forward("";"")
  def forward(self,nextmodule,routerstate):
    self.ctl["user.routing_info"] = nextmodule + ";" + routerstate

class Context:
  def __init__(self,mountpoint,module,initial_sort_policy=None):
    self.mountpoint=mountpoint
    self.main_ctl=xattr.xattr(self.mountpoint + "/mattockfs.ctl")
    self.module_ctl=xattr.xattr(self.mountpoint + "/module/" + module + ".ctl")
    self.instance_ctl=None
    path=self.module_ctl["user.register_instance"]
    print path
    self.instance_ctl = xattr.xattr(self.mountpoint + "/" + path)
    if not initial_sort_policy == None:
      self.instance_ctl["user.sort_policy"] = initial_sort_policy
  def __del__(self):
    if self.instance_ctl != None:
      self.instance_ctl["user.unregister"]="1"
  def set_select_policy(self,pollicy):

  def poll_job(self):
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

  def fs_full_archive(self):

  def module_set_weight(self,weight):

  def module_set_overflow(self,weight):

  def module_instance_count(self):

  def module_instance_count(self):

  def module_do_reset(self):

  def anycast_throttle_info(self,peermodule):

if __name__ == '__main__':
  context=Context("/export/home/rob/dissertation/mattock-dissertation/pymattockfs/mnt","kickstart","K")
  job=context.poll_job()
  kickfile=job.newdata(1234567)
  with open(kickfile,"wb") as f:
    f.seek(0,0)
    f.write("harhar")
    f.seek(0,1234560)
    f.write("HARHAR")
  job.childsubmit("harmodule","router-state-14:11","x-mattock/harhar","har")
  job.done()
    
    
