#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#1. Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
#2. Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#3. All advertising materials mentioning features or use of this software
#   must display the following acknowledgement:
#   This product includes software developed by the <organization>.
#4. Neither the name of the <organization> nor the
#   names of its contributors may be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
#EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
#
#This file contains the API to be used by the future Mattock framework.
#It is a wrapper for the extended attribute interface offered by MattockFS.
#
import xattr
import os
import os.path
import re
from time import sleep
import carvpath

class CarvPathFile:
  def __init__(self,mp,cp):
    self.mp=mp
    self.carvpath=cp
    self.xa=xattr.xattr(mp + "/" + self.carvpath)
  def path_state(self):
    st=self.xa["user.path_state"].split(";")
    return {"hash" : st[0],
            "hash_offset" : int(st[1])}
  def throttle_info(self):
    st=self.xa["user.throttle_info"].split(";")
    return {"normal" : int(st[0]), "dontneed" : int(st[1])}  

class Job:
  def __init__(self,mp,job_ctl):
    self.mp=mp
    self.ctl=xattr.xattr(job_ctl)
    self.router_state=self.ctl["user.routing_info"].split(";")[1]
    self.carvpath=CarvPathFile(mp,self.ctl["user.job_carvpath"])
    self.newdata=None
    self.isdone=False
  def __del__(self):
    self.forward("","")
  def childdata(self,datasize):
    if self.isdone == False:
      self.ctl["user.create_mutable"]=str(datasize)
      self.newdata=self.mp + "/" +self.ctl["user.mutable"]
      return self.newdata
    return None
  def frozen_childdata(self):
    if self.isdone == False:
      return self.ctl["user.frozen_mutable"]
    return None
  def childsubmit(self,carvpath,nextmodule,routerstate,mimetype,extension):
    if self.isdone == False:
      val = carvpath + ";" + nextmodule + ";" +  routerstate + ";" + mimetype + ";" + extension
      self.ctl["user.submit_child"] = val
      self.newdata=None
  def done(self):
    self.forward("","")
  def forward(self,nextmodule,routerstate):
    if self.isdone == False:
      self.ctl["user.routing_info"] = nextmodule + ";" + routerstate
      self.isdone = True

class FsLongPathMap:
  def __init__(self,main_ctl,mp):
    self.main_ctl=main_ctl
    self.cpdir = mp + "/data/"
  def __getitem__(self,key):
    filepath = self.cpdir + key + ".dat"
    if os.path.isfile(filepath):
      ctl=xattr.xattr(filepath)
      return ctl["user.longpath"]
    else:
      raise IndexError("Inknown longpath digest")
  def __setitem__(self,key,val):
    self.main_ctl["user.add_longpath"] = val
  def __contains__(self,key):
    filepath = self.cpdir + key + ".dat"


class Context:
  def __init__(self,mountpoint,module,initial_sort_policy=None):
    self.selectre = re.compile(r'^[SVDWC]{1,5}$')
    self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')    
    self.mountpoint=mountpoint
    self.main_ctl=xattr.xattr(self.mountpoint + "/mattockfs.ctl")
    self.carcpathcontext=carvpath.Context(FsLongPathMap(self.main_ctl,self.mountpoint))
    self.module_ctl=xattr.xattr(self.mountpoint + "/module/" + module + ".ctl")
    self.instance_ctl=None
    path=self.module_ctl["user.register_instance"]
    self.instance_ctl = xattr.xattr(self.mountpoint + "/" + path)
    if not initial_sort_policy == None:
      self.set_sort_policy(initial_sort_policy)
      ok=bool(self.sortre.search(initial_sort_policy))
      if ok:
        self.instance_ctl["user.sort_policy"] = initial_sort_policy
      else:
        raise RuntimeError("Invalid sort policy string")
  def __del__(self):
    if self.instance_ctl != None:
      self.instance_ctl["user.unregister"]="1"
  def set_sort_policy(self,policy):
    ok=bool(self.sortre.search(policy))
    if ok:
      self.instance_ctl["user.sort_policy"] = policy
    else:
      raise RuntimeError("Invalid sort policy string")
  def set_select_policy(self,pollicy):
    ok=bool(self.selectre.search(policy))
    if ok:
      self.instance_ctl["user.select_policy"] = policy
    else:
      raise RuntimeError("Invalid select policy string")
  def poll_job(self):
    try:
      job=self.instance_ctl["user.accept_job"]
      return Job(self.mountpoint,self.mountpoint + "/" + job)
    except:
      return None
  def get_job(self):
    while True:
      job=self.poll_job()
      if job == None:
        sleep(0.05)
      else:
        yield job
  def fs_throttle_info(self):
    st=self.main_ctl["user.throttle_info"].split(";")
    return {"normal": int(st[0]), "dontneed" : int(st[1])}
  def module_set_weight(self,weight):
    self.module_ctl["user.weight"] = str(weight)
  def module_set_overflow(self,overflow):
    self.module_ctl["user.overflow"] = str(overflow)
  def module_instance_count(self):
    return int(self.module_ctl["user.instance_count"])
  #WARNING, will mess with any running module by revoking its instance handle and anything below that.
  def module_reset(self):
    self.module_ctl["user.reset"]="1"
  def anycast_throttle_info(self,peermodule):
    st=self.module_ctl["user.throttle_info"].split(";")
    return {"set_size" : int(st[0]), "set_volume" : int(st[1])}

if __name__ == '__main__':
  mountpoint="/var/mattock/mnt/0"
  print "Regestering kickstart module and setting \"K\" sort policy"
  context=Context(mountpoint,"kickstart","K")
  print "Fetching global throttle info"
  print " * ", context.fs_throttle_info()
  print "Accepting our first job"
  job=context.poll_job()
  print " * carvpath      = ", job.carvpath.carvpath
  print " * path_state    = ",job.carvpath.path_state()
  print " * throttle_info = ",job.carvpath.throttle_info()
  print " * instance_count= ",context.module_instance_count()
  print "Creating new mutable entity within job context"
  kickfile=job.childdata(1234567)
  print " * file =", kickfile
  print "Writing to mutable file"
  with open(kickfile,"r+") as f:
    f.seek(0)
    f.write("harhar")
    f.seek(1234560)
    f.write("HARHAR")
  print "Freezing mutable file"
  frozen=job.frozen_childdata()
  print " * Carvpath =", frozen
  print "Submitting child carvpath to harmodule"
  job.childsubmit(frozen,"harmodule","t1:l11","x-mattock/harhar","har")
  print "Marking parent job as done"
  job.done()
  print 
  print "Checking throttle info for harmodule"
  print " * throttle info = ",context.anycast_throttle_info("harmodule")
  print "Register as harmodule"
  context2=Context(mountpoint,"harmodule")
  print "Setting weight and overflow for harmodule"
  context2.module_set_weight(7)
  context2.module_set_overflow(3)
  print "There should be one job, poll it"
  job2=context2.poll_job()
  if job2 == None:
    print "ERROR, polling the harmodule returned None"
  else:
    print "Fetched job"
    print " * carvpath      = ",job2.carvpath.carvpath
    print " * path_state    =",job2.carvpath.path_state()
    print " * throttle_info =", job2.carvpath.throttle_info()
    print "Submit sub-carvpath 123+1000 as child entity to barmod"
    job2.childsubmit("123+1000","barmod","t9:l4","x-mattock/silly-sparse","sparse")
  print "Forward parent entity to bazmod" 
  job2.forward("bazmod","t18:l6")
  print 
  print "Doing nothing as barmod"
  context3=Context(mountpoint,"barmod")
  job3 = context3.poll_job()
  print " * routing_info : ", job3.router_state
  job3.done
  print 
  print "Doing nothing as bazmod"
  context4=Context(mountpoint,"bazmod")
  job4 = context4.poll_job()
  job4.done
