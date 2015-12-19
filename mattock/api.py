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
#This file contains the Python API to be used by the future Mattock framework.
#It is a wrapper for the extended attribute interface offered by MattockFS.
#
import xattr
import os
import os.path
import re
from time import sleep
import carvpath
import longpathmap

class _CarvPathFile:
  def __init__(self,mp,cp,context):
    self.mp=mp
    self.carvpath=cp
    self.context=context
    self.xa=xattr.xattr(mp + "/" + self.carvpath)
  def as_path(self):
    return self.mp + "/" + self.carvpath
  def as_entity(self):
    return self.context.parse(self.carvpath.split("/")[-1].split(".")[0])
  def __getitem__(self,cp):
    twolevel= self.mp + "/" + self.carvpath.split(".")[0]+"/"+cp
    symlink = self.mp + "/" + self.carvpath.split(".")[0] + "/" + os.readlink(twolevel)
    absolute_symlink=os.path.abspath(symlink)
    cp2 = absolute_symlink[len(self.mp)+1:]
    return _CarvPathFile(self.mp,cp2,self.context)
  def opportunistic_hash(self):
    st=self.xa["user.opportunistic_hash"].split(";")
    return {"hash" : st[0],
            "hash_offset" : int(st[1])}
  def fadvise_status(self):
    st=self.xa["user.fadvise_status"].split(";")
    return {"normal" : int(st[0]), "dontneed" : int(st[1])}  

class _Job:
  def __init__(self,mp,job_ctl,context):
    self.isdone=False
    self.mp=mp
    self.ctl=xattr.xattr(job_ctl)
    rstate=self.ctl["user.routing_info"].split(";")
    self.router_state=rstate[1]
    self.jobmodule=rstate[0]
    self.carvpath=_CarvPathFile(mp,self.ctl["user.job_carvpath"],context)
    self.newdata=None
  def childdata(self,datasize):
    if self.isdone == False:
      self.ctl["user.allocate_mutable"]=str(datasize)
      self.newdata=self.mp + "/" +self.ctl["user.current_mutable"]
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

class _Context:
  def __init__(self,mountpoint,module,context,initial_sort_policy):
    self.selectre = re.compile(r'^[SVDWC]{1,5}$')
    self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')    
    self.mountpoint=mountpoint
    self.main_ctl=xattr.xattr(self.mountpoint + "/mattockfs.ctl")
    self.context=context
    self.module_ctl=xattr.xattr(self.mountpoint + "/module/" + module + ".ctl")
    self.instance_ctl=None
    path=self.module_ctl["user.register_instance"]
    self.instance_ctl = xattr.xattr(self.mountpoint + "/" + path)
    if not initial_sort_policy == None:
      self.set_job_select_policy(initial_sort_policy)
      ok=bool(self.sortre.search(initial_sort_policy))
      if ok:
        self.instance_ctl["user.job_select_policy"] = initial_sort_policy
      else:
        raise RuntimeError("Invalid sort policy string")
  def __del__(self):
    if self.instance_ctl != None:
      self.instance_ctl["user.unregister"]="1"
  def set_job_select_policy(self,policy):
    ok=bool(self.sortre.search(policy))
    if ok:
      self.instance_ctl["user.job_select_policy"] = policy
    else:
      raise RuntimeError("Invalid sort policy string")
  def set_module_select_policy(self,pollicy):
    ok=bool(self.selectre.search(policy))
    if ok:
      self.instance_ctl["user.module_select_policy"] = policy
    else:
      raise RuntimeError("Invalid select policy string")
  def poll_job(self):
    try:
      job=self.instance_ctl["user.accept_job"]
      return _Job(self.mountpoint,self.mountpoint + "/" + job,self.context)
    except:
      return None
  def get_job(self):
    while True:
      job=self.poll_job()
      if job == None:
        sleep(0.05)
      else:
        yield job
  def module_set_weight(self,weight):
    self.module_ctl["user.weight"] = str(weight)
  def module_get_weight(self):
    return int(self.module_ctl["user.weight"])
  def module_set_overflow(self,overflow):
    self.module_ctl["user.overflow"] = str(overflow)
  def module_get_overflow(self):
    return int(self.module_ctl["user.overflow"])
  def module_reset(self):
    self.module_ctl["user.reset"]="1"

class MountPoint:
  def __init__(self,mp):
    self.context=carvpath.Context(longpathmap.LongPathMap())
    self.mountpoint=mp
    self.main_ctl=xattr.xattr(self.mountpoint + "/mattockfs.ctl")
  def register_instance(self,modname,initial_pollicy=None):
    return _Context(self.mountpoint,modname,self.context,initial_pollicy)
  def fadvise_status(self):
    st=self.main_ctl["user.fadvise_status"].split(";")
    return {"normal": int(st[0]), "dontneed" : int(st[1])}
  def full_archive(self):
    return _CarvPathFile(self.mountpoint,self.main_ctl["user.full_archive"],self.context)
  def module_instance_count(self,modulename):
    module_ctl=xattr.xattr(self.mountpoint + "/module/" + modulename + ".ctl")
    return int(module_ctl["user.instance_count"])
  def anycast_status(self,modulename):
    module_ctl=xattr.xattr(self.mountpoint + "/module/" + modulename + ".ctl")
    st=module_ctl["user.anycast_status"].split(";")
    return {"set_size" : int(st[0]), "set_volume" : int(st[1])}
  def full_path(self,entity,ext="dat"):
    return self.mountpoint + "/data/" + str(entity) + "." + ext

