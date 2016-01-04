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

import fuse
import anycast
import stat
import errno 
import random
import re
import carvpath 
import repository
import opportunistic_hash
import sys
import copy
import os
import longpathmap

fuse.fuse_python_api = (0, 2)

#A few constant values for common inode types.
STAT_MODE_DIR = stat.S_IFDIR |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
STAT_MODE_DIR_NOLIST = stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
STAT_MODE_LINK = stat.S_IFLNK |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
STAT_MODE_FILE = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
STAT_MODE_FILE_RO = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

#Generate a decent stat object.
def defaultstat(mode=STAT_MODE_DIR_NOLIST,size=0):
  st = fuse.Stat()
  st.st_blksize= 512
  st.st_mode = mode
  st.st_nlink = 1
  st.st_uid = os.geteuid()
  st.st_gid = os.getegid()
  st.st_size = size
  st.st_blocks = 0
  st.st_atime =  0
  st.st_mtime = 0
  st.st_ctime = 0
  return st


class NoEnt:
  def getattr(self):
    return -errno.ENOENT
  def opendir(self):
    return -errno.ENOENT
  def readlink(self):
    return -errno.ENOENT
  def listxattr(self):
    return -errno.ENOENT
  def getxattr(self,name, size):
    return -errno.ENOENT
  def setxattr(self,name, val):
    return -errno.ENOENT
  def open(self,flags,path):
    return -errno.ENOENT
class TopDir:
  def getattr(self):
    return defaultstat(STAT_MODE_DIR)
  def opendir(self):
    return 0
  def readdir(self):
    yield fuse.Direntry("mattockfs.ctl")
    yield fuse.Direntry("carvpath")
    yield fuse.Direntry("actor")
    yield fuse.Direntry("worker")
    yield fuse.Direntry("job")
    yield fuse.Direntry("mutable")
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return []
  def getxattr(self,name, size):
    return -errno.ENODATA
  def setxattr(self,name, val):
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class NoList:
  def getattr(self):
    return  defaultstat(STAT_MODE_DIR_NOLIST)
  def opendir(self):
    return -errno.EPERM
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return []
  def getxattr(self,name, size):
    return -errno.ENODATA
  def setxattr(self,name, val):
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class TopCtl:
  def __init__(self,rep,context):
    self.rep=rep
    self.context=context
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.fadvise_status","user.full_archive"]
  def getxattr(self,name, size):
    if name == "user.fadvise_status":
      return ";".join(map(lambda x: str(x),self.rep.getTopThrottleInfo()))
    if name == "user.full_archive":
      return "carvpath/" + str(self.rep.top.topentity) + ".raw"
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name in ("user.fadvise_status","user.full_archive"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class ActorCtl:
  def __init__(self,mod):
    self.mod=mod
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.weight","user.overflow","user.anycast_status","user.worker_count","user.reset","user.register_worker"]
  def getxattr(self,name, size):
    if name == "user.weight":
      return str(self.mod.weight)
    if name == "user.overflow":
      return str(self.mod.overflow)
    if name == "user.anycast_status":
      return ";".join(map(lambda x: str(x),self.mod.throttle_info()))
    if name == "user.worker_count":
      return str(self.mod.worker_count())
    if name == "user.reset":
      return "0"
    if name == "user.register_worker":
      if size == 0:
        return 82
      else:
        return "worker/" + self.mod.register_worker() + ".ctl" 
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name == "user.weight":
      try:
        asnum=int(val)
      except ValueError:
        return 0
      self.mod.weight=asnum
      return 0
    if name == "user.overflow":
      try:
        asnum=int(val)
      except ValueError:
        return 0
      self.mod.overflow=asnum
      return 0
    if name in ("user.anycast_status","user.worker_count","user.register_worker"):
      return -errno.EPERM
    if name == "user.reset":
      if val == "1":
        self.mod.reset() 
      return 0
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class ActorInf:
  def __init__(self,mod):
    self.mod=mod
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.anycast_status","user.worker_count"]
  def getxattr(self,name, size):
    if name == "user.anycast_status":
      return ";".join(map(lambda x: str(x),self.mod.throttle_info()))
    if name == "user.worker_count":
      return str(self.mod.worker_count())
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name in ("user.anycast_status","user.worker_count"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class WorkerCtl:
  def __init__(self,worker,sortre,selectre):
    self.worker=worker
    self.sortre=sortre
    self.selectre=selectre
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.job_select_policy","user.actor_select_policy","user.unregister","user.accept_job"]
  def getxattr(self,name, size):
    if name == "user.job_select_policy":
      return self.worker.job_select_policy
    if name == "user.actor_select_policy":
      return self.worker.module_select_policy
    if name == "user.unregister":
      return "0"
    if name == "user.accept_job":
      if size == 0:
        return 77
      else:
        job=self.worker.accept_job()
        if job == None:
          return -errno.ENODATA
        return "job/" + job + ".ctl"    
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name == "user.job_select_policy":
       ok=bool(self.sortre.search(val))
       if ok:
         self.worker.job_select_policy=val
       return 0
    if name == "user.actor_select_policy":
       ok=bool(self.selectre.search(val))
       if ok:
         self.worker.module_select_policy=val
       return 0
    if name == "user.unregister":
      if val == "1":
        self.worker.unregister()
      return 0
    if name == "user.accept_job":
       return -errno.EPERM 
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class JobCtl:
  def __init__(self,job):
    self.job=job
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self): 
    return ["user.routing_info","user.submit_child","user.allocate_mutable","user.frozen_mutable","user.current_mutable","user.job_carvpath"]
  def getxattr(self,name, size):
    if name == "user.routing_info":
      return self.job.actorname + ";" + self.job.router_state
    if name == "user.submit_child":
      return ""
    if name == "user.allocate_mutable":
      return ""
    if name == "user.current_mutable":
      rval= self.job.get_mutable()
      if rval == None:
        return ""
      return "mutable/" + rval + ".dat"
    if name == "user.frozen_mutable":
      if size == 0:
        return 81
      frozen=self.job.get_frozen_mutable()
      if frozen != None:
        return "carvpath/" + self.job.get_frozen_mutable() + ".dat"
      return -errno.ENODATA
    if name == "user.job_carvpath":
      return "carvpath/" + self.job.carvpath + "." + self.job.file_extension
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name == "user.routing_info":
      if ";" in val:
        parts=val.split(";")
        if self.job.actors.validactorname(parts[0]):
          self.job.next_hop(parts[0],parts[1])
      return 0
    if name == "user.submit_child":
      parts = val.split(";")
      if len(parts) == 5:
        #carvpath,nexthop,routerstate,mime,ext
        self.job.submit_child(parts[0],parts[1],parts[2],parts[3],parts[4])
      return 0
    if name == "user.allocate_mutable":
      self.job.create_mutable(int(val))
      return 0
    if name in ("user.frozen_mutable","user.job_carvpath"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM
class MutableCtl:
  def __init__(self,carvpath,rep,context):
    self.rep=rep
    self.carvpath=carvpath
    self.context=context
  def getattr(self):
    size=self.context.parse(self.carvpath).totalsize
    return  defaultstat(STAT_MODE_FILE_RO,size)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return []
  def getxattr(self,name, size):
    return -errno.ENODATA
  def setxattr(self,name, val):
    return -errno.ENODATA
  def open(self,flags,path):
    return self.rep.open(self.carvpath,path) 
class CarvPathFile:
  def __init__(self,carvpath,rep,context,actors):
    self.carvpath=carvpath
    self.rep=rep
    self.context=context
    self.actors=actors
    self.badflags = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC | os.O_EXCL
  def getattr(self):
    size=self.context.parse(self.carvpath).totalsize
    return  defaultstat(STAT_MODE_FILE_RO,size)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.opportunistic_hash","user.fadvise_status"]
  def getxattr(self,name, size):
    if name == "user.opportunistic_hash":
      offset="0"
      hashresult=""
      if self.carvpath in self.actors.rep.stack.ohashcollection.ohash:
        ohash = self.actors.rep.stack.ohashcollection.ohash[self.carvpath].ohash
        offset=str(ohash.offset)
        hashresult=ohash.result
      return hashresult + ";" + offset
    if name == "user.fadvise_status":
      return ";".join(map(lambda x: str(x),self.actors.rep.stack.carvpath_throttle_info(self.carvpath)))
  def setxattr(self,name, val):
    if name in ("user.opportunistic_hash","user.fadvise_status"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags,path):
    if (flags & self.badflags) != 0:
      return -errno.EPERM
    return self.rep.open(self.carvpath,path)
class CarvPathLink:
  def __init__(self,cp,ext):
    if ext == None:
      self.link = "../" + cp
    else:
     self.link = "../" + cp + "." + ext
  def getattr(self):
    return  defaultstat(STAT_MODE_LINK)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return self.link
  def listxattr(self):
    return []
  def getxattr(self,name, size):
    return -errno.ENODATA
  def setxattr(self,name, val):
    return -errno.ENODATA
  def open(self,flags,path):
    return -errno.EPERM

class MattockFS(fuse.Fuse):
    def __init__(self,dash_s_do,version,usage,dd,lpdb,journal,provenance_log,ohash_log,refcount_log):
      super(MattockFS, self).__init__(version=version,usage=usage,dash_s_do=dash_s_do)
      self.longpathdb =lpdb 
      self.context=carvpath.Context(self.longpathdb)
      self.topdir=TopDir()
      self.nolistdir=NoList()
      self.selectre = re.compile(r'^[SVDWC]{1,5}$')
      self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')
      self.archive_dd = dd
      self.rep=repository.Repository(reppath=self.archive_dd,context=self.context,ohash_log=ohash_log,refcount_log=refcount_log)
      self.ms=anycast.Actors(rep=self.rep,journal=journal,provenance=provenance_log,context=self.context,stack=self.rep.stack,col=self.rep.col)
      self.topctl=TopCtl(self.rep,self.context)
      self.needinit=True
    def parsepath(self,path):
        if path == "/":
          return self.topdir
        tokens=path[1:].split("/")
        if len(tokens) > 3:
          return None
        if tokens[0] in ("carvpath","actor","worker","job","mutable","mattockfs.ctl"):
          if len(tokens) >2 and tokens[0] != "carvpath":
            return NoEnt()
          if len(tokens) == 1:
            if tokens[0] == "mattockfs.ctl":
              return self.topctl
            return self.nolistdir
          if tokens[0] == "carvpath":
            lastpart=tokens[1].split(".")
            if len(lastpart) > 2:
              return NoEnt()
            topcp=lastpart[0]
            if not self.rep.validcarvpath(topcp):
              return NoEnt()
            if len(tokens) == 2:
              if len(lastpart) == 2:
                return CarvPathFile(topcp,self.rep,self.context,self.ms)
              if len(lastpart) > 2:
                  return NoEnt()
              return self.nolistdir
            #must be 3 now
            lastpart=tokens[2].split(".")
            if len(lastpart) > 2: 
              return NoEnt()
            ext=None
            if len(lastpart) == 2:
              ext=lastpart[1]
            link=self.rep.flatten(topcp,lastpart[0])
            if link != None:
              return CarvPathLink(link,ext)
            return NoEnt()
          lastpart=tokens[1].split(".")
          if len(lastpart) !=2:
            return NoEnt()
          handle = lastpart[0]
          extension = lastpart[1]
          if extension == "ctl":
            if tokens[0] == "actor":
              if self.ms.validactorname(handle):
                return ActorCtl(self.ms[handle])
              return NoEnt()
            if tokens[0] == "worker":
              if self.ms.validworkercap(handle):
                return WorkerCtl(self.ms.workers[handle],self.sortre,self.selectre)
              return NoEnt()
            if tokens[0] == "job":
              if self.ms.validjobcap(handle):
                return JobCtl(self.ms.jobs[handle])
              return NoEnt()
            return NoEnt()
          if extension == "dat" and  tokens[0] == "mutable":
            if self.ms.validnewdatacap(handle):
              return MutableCtl(self.ms.newdata[handle],self.rep,self.context)
          if extension == "inf" and tokens[0] == "actor":
            if self.ms.validactorname(handle):
              return ActorInf(self.ms[handle])
            return NoEnt()
          return NoEnt()
        return NoEnt()
    def getattr(self, path):
      return self.parsepath(path).getattr()
    def setattr(self,path,hmm):
      return 0
    def opendir(self, path):
      return self.parsepath(path).opendir()
    def readdir(self, path, offset):
      return self.parsepath(path).readdir()
    def releasedir(self,path):
      return 0
    def readlink(self,path):
      return self.parsepath(path).readlink()
    def listxattr(self, path,huh):
      return self.parsepath(path).listxattr()
    def getxattr(self, path, name, size):
      rval=self.parsepath(path).getxattr(name,size)
      if size == 0:
         if not isinstance(rval, int):
          rval=len(rval)
      return rval
    def setxattr(self, path, name, val, more):
      return self.parsepath(path).setxattr(name,val)
    def main(self,args=None):
      fuse.Fuse.main(self, args)
    def open(self, path, flags):
      rval = self.parsepath(path).open(flags,path)
      return rval
    def release(self, path, fh):
      return self.rep.close(path)
    def read(self, path, size, offset):
      return self.rep.read(path,offset,size)
    def write(self,path,data,offset):
      rval= self.rep.write(path,offset,data)
      return rval
    def truncate(self,path,len,fh=None):
      return -errno.EPERM
    def flush(self,path):
      self.rep.flush()

def run():
    mattockdir="/var/mattock"
    if not os.path.isdir(mattockdir):
      print "ERROR: ", mattockdir, "should exist and be owned by the mattockfs user"
      sys.exit()
    for subdir in ["archive","log","mnt"]:
      sd=mattockdir + "/" + subdir
      if not os.path.isdir(sd):
        try:
          os.mkdir(sd)
        except:
          print "ERROR: Can't create missing",sd,";", mattockdir, "should be owned by the mattockfs user"
          sys.exit()
    mattockitem="0"
    mp=mattockdir + "/mnt/" + mattockitem
    if not os.path.isdir(mp):
      try:
        os.mkdir(mp)
      except:
        print "ERROR: Can't create missing",mp,";", mattockdir+"/mnt should be owned by the mattockfs user"
        sys.exit()
    dd = mattockdir + "/archive/" + mattockitem + ".dd"
    journal = mattockdir + "/log/" + mattockitem + ".journal"
    provenance_log = mattockdir + "/log/" + mattockitem + ".provenance"
    ohash_log = mattockdir + "/log/" + mattockitem + ".ohash"
    refcount_log = mattockdir + "/log/" + mattockitem + ".refcount"
    mp=mattockdir + "/mnt/" + mattockitem
    sys.argv.append(mp)
    mattockfs = MattockFS(version = '%prog ' + '0.3.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle',
               dd=dd,
               lpdb=longpathmap.LongPathMap(),
               journal=journal,
               provenance_log=provenance_log,
               ohash_log=ohash_log,
               refcount_log=refcount_log)
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()

if __name__ == '__main__':
  run()
