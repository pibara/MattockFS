#!/usr/bin/python
import fuse
import stat
import errno 
import random
import re
import pycarvpath 
import sys
import copy

fuse.fuse_python_api = (0, 2)

try:
    from pyblake2 import blake2b
except ImportError:
    import sys
    print("")
    print("\033[93mERROR:\033[0m Pyblake2 module not installed. Please install blake2 python module. Run:")
    print("")
    print("    sudo pip install pyblake2")
    print("")
    sys.exit()

class Repository:
  def __init__(self,ddfile,context):
    self.cpcontext=context
    self.repository=self.cpcontext.open_repository(ddfile)
    self.openfiles={}
    self.lastfd=1
  def full_archive_size(self):
    return self.repository.top.size
  def validcarvpath(self,cp):
    try:
      ent=self.cpcontext.parse(cp)
      rval=self.repository.top.test(ent)
      return rval
    except:
      return False
  def flatten(self,basecp,subcp):
    try:
      ent=self.cpcontext.parse(basecp + "/" + subcp)
      return str(ent)
    except:
      return None
  def anycast_set_volume(self,anycast):
    volume=0
    for jobid in anycast:
      volume += self.cpcontext.parse(anycast[jobid].carvpath).totalsize
    return volume 
  def anycast_best(self,modulename,anycast,sort_policy):
    if len(anycast) > 0:
      cp2key={}
      for anycastkey in anycast.keys():
        cp=anycast[anycastkey].carvpath
        cp2key[cp]=anycastkey
      bestcp=self.repository.rep.priority_custompick(sort_policy,intransit=cp2key.keys())  
      return cp2key[bestcp]
    return None
  def getTopThrottleInfo(self):
    totalsize=self.repository.top.size
    ioa = 0 #FIXME, we should keep track of data that is explicitly read/written
    normal=self.repository.volume()
    willneed=0 #FIXME: we should allow the framework to mark chunks as willneed.
    dontneed=totalsize-willneed-normal
    return (normal,willneed,dontneed,ioa) 
  def anycast_best_modules(self,modulesstate,moduleset,letter):
    fetchvolume=False
    if letter in "VDC":
      fetchvolume=True
    bestmodules=set()
    bestval=0
    for module in moduleset.modules.keys():
      anycast = moduleset.modules[module].anycast
      overflow = moduleset.modules[module].overflow
      if len(anycast) > overflow:
        volume=0
        if fetchvolume:
          volume=self.anycast_set_volume(anycast)
        val=0
        if letter == "S":
          val=len(anycast)
        if letter == "V":
          val=volume
        if letter == "D":
          if volume > 0:
            val=float(len(anycast))/float(volume)
          else:
            if len(anycast) > 0:
               return 100*len(anycast)
        if letter == "W":
          val=modulesstate[module].weight
        if letter == "C":
          if volume > 0:
            val=float(modulesstate[module].weight)*float(len(anycast))/float(volume)
          else:
            if len(anycast) > 0:
              return 100*len(anycast)*modulesstate[module].weight
        if val > bestval:
          bestval=val
          bestmodules=set()
        if val == bestval:
          bestmodules.add(module)
    return bestmodules
  def openro(self,carvpath,path):
    ent=self.cpcontext.parse(carvpath)
    if path in self.openfiles:
      self.openfiles[path].refcount += 1
    else :
      ent=self.cpcontext.parse(carvpath)
      self.openfiles[path]=self.repository.openro(carvpath,ent)
    return 0
  def read(self,path,offset,size):
    return self.openfiles[path].read(offset,size)
  def close(self,path):
    self.openfiles[path].refcount -= 1
    if self.openfiles[path].refcount < 1:
      del self.openfiles[path]
    return 0
 
    
class ModuleInstance:
  def __init__(self,modulename,instancehandle,module):
    self.module=module
    self.modulename=modulename
    self.instancehandle=instancehandle
    self.lastjobno=0
    self.currentjob=None
    self.currentjobhandle=None
    self.select_policy="S"
    self.sort_policy="HrdS"
  def active(self):
    return self.currentjob != None
  def teardown(self):
    if self.currentjob != None:
      self.currentjob.commit()
  def unregister(self):
    self.module.unregister(self.instancehandle)
  def accept_job(self):
    if self.currentjob != None:
      self.currentjob.commit()
    if self.sort_policy=="K":
      self.currentjob=self.module.get_kickjob() 
    else:
      self.currentjob=self.module.anycast_pop(self.select_policy,self.sort_policy)
    if self.currentjob != None:
      return self.currentjob.jobhandle
    return None

class Job:
  def __init__(self,jobhandle,modulename,carvpath,router_state,mime_type,file_extension,allmodules,provenance=copy.deepcopy(list())):
    self.jobhandle=jobhandle
    self.modulename=modulename
    self.carvpath=carvpath
    self.router_state=router_state
    self.mime_type=mime_type
    self.file_extension=file_extension
    self.submit_info=[]
    self.allmodules=allmodules
    self.provenance = provenance
    provenancerecord={}
    provenancerecord["job"]=jobhandle
    provenancerecord["module"]=modulename
    if len(provenance) == 0:
      provenancerecord["carvpath"]=carvpath
      provenancerecord["mime_type"]=mime_type
    self.provenance.append(provenancerecord)
  def commit(self):
    if self.jobhandle in self.allmodules.jobs:
      print "PROVENANCE:",self.provenance
      del self.allmodules.jobs[self.jobhandle] 
  def next_hop(self,module,state):
    self.allmodules[module].anycast_add(self.carvpath,state,self.mime_type,self.file_extension,self.provenance)
    del self.allmodules.jobs[self.jobhandle]

class ModuleState:
  def __init__(self,modulename,strongname,allmodules,rep):
    self.name=modulename
    self.instances={}
    self.anycast={}
    self.lastinstanceno=0
    self.lastjobno=0
    self.secret=strongname
    self.weight=100              #rw extended attribute
    self.overflow=10             #rw extended attribute
    self.allmodules=allmodules
    self.rep=rep
  def register_instance(self):   #read-only (responsibility accepting) extended attribute.
    instanceno=self.lastinstanceno
    self.lastinstanceno += 1
    rval = "I" + blake2b("I" + hex(instanceno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.instances[rval]=ModuleInstance(self.name,rval,self)
    self.allmodules.instances[rval]=self.instances[rval]
    return rval
  def unregister(self,handle):
    if handle in self.instances:
      self.instances[handle].teardown()
      del self.instances[handle]
      del self.allmodules.instances[handle]
  def reset(self):             #writable extended attribute (setting to one results in reset)
    handles=self.instances.keys()
    for handle in handles:
      self.unregister(handle)
  def instance_count(self):   #read-only extended attribute
    return len(self.instances)
  def throttle_info(self):    #read-only extended attribute
    set_size=len(self.anycast)
    set_volume=self.rep.anycast_set_volume(self.anycast)
    return (set_size,set_volume)
  def anycast_add(self,carvpath,router_state,mime_type,file_extension):   #FIXME: UNTESTED !!!
    jobno=self.lastjobno
    self.lastjobno += 1
    jobhandle = "J" + blake2b("J" + hex(jobno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.anycast[jobhandle]=Job(jobhandle,self.name,carvpath,router_state,mime_type,file_extension,self.allmodules,[])
    self.allmodules.path_state[carvpath]="anycast"
    self.allmodules.path_module[carvpath]=self.name
  def get_kickjob(self):
    jobno=self.lastjobno
    self.lastjobno += 1
    jobhandle = "J" + blake2b("J" + hex(jobno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.allmodules.jobs[jobhandle]=Job(jobhandle,"kickstart","S0","","application/x-zerosize","empty",self.allmodules,[])
    return self.allmodules.jobs[jobhandle]
  def anycast_pop(self,sort_policy,select_policy="S"): #FIXME: UNTESTED !!!
    if self.name != "loadbalance":
      best=self.rep.anycast_best(self.name,self.anycast,sort_policy)
      if best != None:
        self.allmodules[best]=self.anycast.pop(best)
        self.allmodules.path_state[self.allmodules[best].carvpath]="pending"
        self.allmodules.path_module[self.allmodules[best].carvpath]=self.name
        return self.allmodules[best]
    else:
      best=self.allmodules.selectmodule(select_policy)
      if best != None:
        best=anycast_pop(sort_policy,select_policy)
        self.allmodules.path_state[best.carvpath]="migrating"
        self.allmodules.path_module[best.carvpath]=self.name
        return best
      return None 

class ModulesState:
  def __init__(self,rep):
    self.rep=rep
    self.modules={}
    self.instances={}
    self.jobs={}
    self.newdata={}
    self.path_state={} 
    self.path_module={}
    random.seed()
    self.rumpelstiltskin=''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(0,64))
  def __getitem__(self,key):
    if not key in self.modules:
      strongname = "M" + blake2b("M" +key,digest_size=32,key=self.rumpelstiltskin).hexdigest()
      self.modules[key]=ModuleState(key,strongname,self,self.rep)
    return self.modules[key]
  def selectmodule(self,select_policy):
    moduleset=self.modules.keys()
    if len(moduleset) == 0:
      return None
    if len(moduleset) == 1:
      return moduleset[0]
    for letter in select_policy:
      moduleset=self.rep.anycast_best_modules(self,moduleset,letter) 
      if len(moduleset) == 1:
        return moduleset[0]
    return moduleset[0]
  def validmodulename(self,modulename):
    if len(modulename) < 2:
      return False
    if len(modulename) > 40:
      return False
    if not modulename.isalpha() and  modulename.islower():
      return False
    return True
  def validinstancecap(self,handle):
    if handle in self.instances:
      return True
    return False
  def validjobcap(self,handle):
    if handle in self.jobs:
      return True
    return False  
  def validnewdatacap(self,handle):
    if handle in self.newdata:
      return True
    return False

STAT_MODE_DIR = stat.S_IFDIR |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
STAT_MODE_DIR_NOLIST = stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
STAT_MODE_LINK = stat.S_IFLNK |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
STAT_MODE_FILE = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
STAT_MODE_FILE_RO = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

def defaultstat(mode=STAT_MODE_DIR_NOLIST,size=0):
  st = fuse.Stat()
  st.st_blksize= 512
  st.st_mode = mode
  st.st_nlink = 1
  st.st_uid = 0
  st.st_gid = 0
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
  def open(self,flags):
    return -errno.ENOENT
class TopDir:
  def getattr(self):
    return defaultstat()
  def opendir(self):
    return 0
  def readdir(self):
    yield fuse.Direntry("mattockfs.ctl")
    yield fuse.Direntry("data")
    yield fuse.Direntry("module")
    yield fuse.Direntry("instance")
    yield fuse.Direntry("job")
    yield fuse.Direntry("newdata")
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return []
  def getxattr(self,name, size):
    return -errno.ENODATA
  def setxattr(self,name, val):
    return -errno.ENODATA
  def open(self,flags):
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
  def open(self,flags):
    return -errno.EPERM
class TopCtl:
  def __init__(self,rep):
    self.rep=rep
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.throttle_info","user.full_archive"]
  def getxattr(self,name, size):
    if name == "user.throttle_info":
      return ";".join(map(lambda x: str(x),self.rep.getTopThrottleInfo()))
    if name == "user.full_archive":
      return "data/" + str(self.rep.repository.top.topentity) + ".raw"
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name in ("user.throttle_info","user.full_archive"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags):
    return -errno.EPERM
class ModuleCtl:
  def __init__(self,mod):
    self.mod=mod
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.weight","user.overflow","user.throttle_info","user.instance_count","user.reset","user.register_instance"]
  def getxattr(self,name, size):
    if name == "user.weight":
      return str(self.mod.weight)
    if name == "user.overflow":
      return str(self.mod.overflow)
    if name == "user.throttle_info":
      return ";".join(map(lambda x: str(x),self.mod.throttle_info()))
    if name == "user.instance_count":
      return str(self.mod.instance_count())
    if name == "user.reset":
      return "0"
    if name == "user.register_instance":
      if size == 0:
        return 82
      else:
        return "instance/" + self.mod.register_instance() + ".ctl" 
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
    if name in ("user.throttle_info","user.instance_count","user.register_instance"):
      return -errno.EPERM
    if name == "user.reset":
      if val == "1":
        self.mod.reset() 
      return 0
    return -errno.ENODATA
  def open(self,flags):
    return -errno.EPERM
class InstanceCtl:
  def __init__(self,instance,sortre,selectre):
    self.instance=instance
    self.sortre=sortre
    self.selectre=selectre
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.sort_policy","user.select_policy","user.unregister","user.active","user.accept_job"]
  def getxattr(self,name, size):
    if name == "user.sort_policy":
      return self.instance.sort_policy
    if name == "user.select_policy":
      return self.instance.select_policy
    if name == "user.unregister":
      return "0"
    if name == "user.active":
      if self.instance.active():
        return "1"
      return "0" 
    if name == "user.accept_job":
      if size == 0:
        return 77
      else:
        job=self.instance.accept_job()
        if job == None:
          return -errno.ENODATA
        return "job/" + job + ".ctl"    
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name == "user.sort_policy":
       ok=bool(self.sortre.search(val))
       if ok:
         self.instance.sort_policy=val
       return 0
    if name == "user.select_policy":
       ok=bool(self.selectre.search(val))
       if ok:
         self.instance.select_policy=val
       return 0
    if name == "user.unregister":
      if val == "1":
        self.instance.unregister()
      return 0
    if name in ("user.active","user.accept_job"):
       return -errno.EPERM 
    return -errno.ENODATA
  def open(self,flags):
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
    return ["user.routing_info","user.derive_child_entity","user.create_mutable_child_entity","user.set_child_submit_info","user.active_child","user.job_carvpath"]
  def getxattr(self,name, size):
    if name == "user.routing_info":
      return self.job.module_name + ";" + self.job.router_state
    if name == "user.derive_child_entity":
      return -errno.EPERM
    if name == "user.create_mutable_child_entity":
      return -errno.EPERM
    if name == "user.set_child_submit_info":
      return -errno.EPERM
    if name == "user.active_child":
      return "FIXME"
    if name == "user.job_carvpath":
      return "data/" + self.job.carvpath + "." + self.job.file_extension
    return -errno.ENODATA
  def setxattr(self,name, val):
    if name == "user.routing_info":
      if ";" in val:
        parts=val.split(";")
        if self.job.allmodules.validmodulename(parts[0]):
          self.job.next_hop(parts[0],parts[1])
      return 0
    if name == "user.derive_child_entity":
      return 0
    if name == "user.create_mutable_child_entity":
      return 0
    if name == "user.set_child_submit_info":
      return 0
    if name == "user.active_child":
      return 0
    if name == "user.job_carvpath":
      return 0
    return -errno.ENODATA
  def open(self,flags):
    return -errno.EPERM
class NewDataCtl:
  def __init__(self,newdata):
    self.newdata=newdata
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
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
  def open(self,flags):
    return -errno.EPERM
class CarvPathFile:
  def __init__(self,carvpath,rep,context,modules):
    self.carvpath=carvpath
    self.rep=rep
    self.context=context
    self.modules=modules
  def getattr(self):
    size=self.context.parse(self.carvpath).totalsize
    return  defaultstat(STAT_MODE_FILE_RO,size)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
  def listxattr(self):
    return ["user.path_state","user.throttle_info"]
  def _lookup(self,module):
    for anycastkey in ms[module].anycast.keys():
      if ms.modules[modulekey].anycast[anycastkey].carvpath==self.carvpath:
        job = ms.modules[modulekey].anycast[anycastkey]
        return (job.mime_type,job.file_extension)
    return ("","")
  def getxattr(self,name, size):
    if name == "user.path_state":
      module=""
      state="none"
      jobmeta=""
      offset="0"
      mimetype=""
      extension=""
      hashresult="INCOMPLETE-OPPORTUNISTIC_HASHING"
      if self.carvpath in self.modules.path_module:
        module=self.modules.path_module[self.carvpath]
        state=self.modules.path_state[self.carvpath]
        jobmeta=self._lookup(module)
        mimetype=jobmeta[0]
        extension=jobmeta[1]
      if self.carvpath in self.modules.rep.repository.box.ohash:
        ohash = self.modules.rep.repository.box.ohash[self.carvpath].ohash
        offset=str(ohash.offset)
        hashresult=ohash.result
        if state == "none":
          state="open"
      return state + ";" + module + ";" + mimetype + ";" + extension + ";" + hashresult + ";" + offset
    if name == "user.throttle_info":
      return ";;;" #FIXME, should return fadv info (normal,willneed,dontneed,io_access)
  def setxattr(self,name, val):
    if name in ("user.state","user.throttle_info"):
      return -errno.EPERM
    return -errno.ENODATA
  def open(self,flags,path):
    return self.rep.openro(self.carvpath,path)
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
  def open(self,flags):
    return -errno.EPERM

class MattockFS(fuse.Fuse):
    def __init__(self,dash_s_do,version,usage,dd):
      super(MattockFS, self).__init__(version=version,usage=usage,dash_s_do=dash_s_do)
      longpathdb ={} #FIXME: persistent or distributed longpath storage is desired.
      self.context=pycarvpath.Context(longpathdb)
      self.topdir=TopDir()
      self.nolistdir=NoList()
      self.selectre = re.compile(r'^[SVDWC]{1,5}$')
      self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')
      self.archive_dd = dd
      self.rep=Repository(self.archive_dd,self.context)
      self.ms=ModulesState(self.rep)
      self.topctl=TopCtl(self.rep)
      self.needinit=True
    def parsepath(self,path):
        if path == "/":
          return self.topdir
        tokens=path[1:].split("/")
        if len(tokens) > 3:
          return None
        if tokens[0] in ("data","module","instance","job","newdata","mattockfs.ctl"):
          if len(tokens) >2 and tokens[0] != "data":
            return NoEnt()
          if len(tokens) == 1:
            if tokens[0] == "mattockfs.ctl":
              return self.topctl
            return self.nolistdir
          if tokens[0] == "data":
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
            if tokens[0] == "module":
              if self.ms.validmodulename(handle):
                return ModuleCtl(self.ms[handle])
              return NoEnt()
            if tokens[0] == "instance":
              if self.ms.validinstancecap(handle):
                return InstanceCtl(self.ms.instances[handle],self.sortre,self.selectre)
              return NoEnt()
            if tokens[0] == "job":
              if self.ms.validjobcap(handle):
                return JobCtl(self.ms.jobs[handle])
              return NoEnt()
            return NoEnt()
          if extension == "dat" and  tokens[0] == "newdata" and self.ms.validnewdatacap(handle):
            return NewDataCtl(self.ms.newdata[handle])
          return NoEnt()
        return NoEnt()
    def getattr(self, path):
      return self.parsepath(path).getattr()
    def opendir(self, path):
      return self.parsepath(path).opendir()
    def readdir(self, path, offset):
      return self.parsepath(path).readdir()
    def readlink(self,path):
      return self.parsepath(path).readlink()
    def listxattr(self, path,huh):
      return self.parsepath(path).listxattr()
    def getxattr(self, path, name, size):
      return self.parsepath(path).getxattr(name,size)
    def setxattr(self, path, name, val, more):
      return self.parsepath(path).setxattr(name,val)
    def main(self,args=None):
      fuse.Fuse.main(self, args)
    def open(self, path, flags):
      return self.parsepath(path).open(flags,path)
    def release(self, path, fh):
      return self.rep.close(path)
    def read(self, path, size, offset):
      return self.rep.read(path,offset,size)

if __name__ == '__main__':
    dd="/var/mattock/archive/0.dd"
    isoption=False
    for arg in sys.argv:
      if isoption and "archive_dd" in arg:
        dd=arg[11:]
      if arg == "-o":
        isoption=True
      else:
        isoption=False
    mattockfs = MattockFS(version = '%prog ' + '0.1.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle',dd=dd)
    #Seems option parsing is a bit tricky, we add it to the expected options so we don't croke.
    mattockfs.parser.add_option(mountopt="archive_dd",
                                metavar="DD",
                                default=mattockfs.archive_dd,
                                help="Path of the archive dd file to use [default: %default]")
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()
