#!/usr/bin/python
import fuse
import stat
import errno 
import random
import re

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
  def __init__(self):
    pass
  def full_archive_size(self):
    return 1234567890

class CarvpathBox:
  def __init__(self,rep):
    self.rep=rep
  def getPathState(self,cp):
    return ("INCOMPLETE",1821)
  def anycast_best(self,modulename,anycast,sort_policy,select_policy):
    return anycast.keys()[0] #FIXME
  def anycast_set_volume(self,anycast):
    return 1000 #FIXME
  def anycast_best_modules(self,allmodules,moduleset,letter):
    return (moduleset.modules[moduleset.modules.keys()[0]])
  def throttle_state(self):
    return (10,0,0,0,0,0)
  def getPathThottleInfo(self,cp):
    return (8000000,0,900000000,1689990,12,19000)
  def validcarvpath(self,cp):
    return True
  def flatten(self,basecp,subcp):
    return "8756+1744"
  def carvpathSize(self,cp):
    return 2345



class MattockFSCore:
  def getPathState(self,cp):
    return "anycast;antiword;application/ms-word;doc;INCOMPLETE;4096"
  def createSubJobFromMutable(self,job,mutable):
    return "JOBjobJOB"
  def validExtension(self,ext):
    return True
  def validMimeType(self,mimetype):
    return True
  def getInstanceSortPollicy(self,handle):
    return "OD"
  def getInstanceSelectPollicy(self,handle):
    return "V"
  def setInstanceSortPollicy(self,handle,val):
    return "OD"
  def setInstanceSelectPollicy(self,handle,val):
    return "V"
  def deriveChildEntity(self,handle,val):
    return
  def createMutableChildEntity(self,handle,ival):
    return
  def getTopThrottleInfo(self):
    return (10,0,0,0,0,0)
    
class ModuleInstance:
  def __init__(self,modulename,instancehandle,module):
    self.modulename=modulename
    self.instancehandle=instancehandle
    self.module=module
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
    module.unregister(self.instancehandle)
  def accept_job(self):
    if self.currentjob != None:
      self.currentjob.commit()
    self.currentjob=self.anycast.pop(self.modulename,self.select_policy,self.sort_policy)
    if self.currentjob != None:
      jobno=self.lastjobno
      self.lastjobno += 1
      jobhandle = "J" + blake2b(hex(jobno)[2:].zfill(16),digest_size=32,key=self.instancehandle).hexdigest()
    
class Job:
  def __init__(self,jobhandle,modulename,carvpath,router_state,mime_type,file_extension):
    self.jobhandle=jobhandle
    self.modulename=modulename
    self.carvpath=carvpath
    self.router_state=router_state
    self.mime_type=mime_type
    self.file_extension=file_extension
    self.routing_info=""
    self.submit_info=[]

class ModuleState:
  def __init__(self,modulename,strongname,allmodules):
    self.name=modulename
    self.instances={}
    self.anycast={}
    self.lastinstanceno=0
    self.lastjobno=0
    self.secret=strongname
    self.weight=100              #rw extended attribute
    self.overflow=10             #rw extended attribute
    self.allmodules=allmodules
  def register_instance(self):   #read-only (responsibility accepting) extended attribute.
    instanceno=self.lastinstanceno
    self.lastinstanceno += 1
    rval = "I" + blake2b("I" + hex(instanceno)[2:].zfill(16),digest_size=32,key=self.secret).hexdigest()
    self.instances[rval]=ModuleInstance(self.name,rval,module)
    self.allmodules.instances[rval]=self.instances[rval]
  def unregister(self,handle):
    if handle in self.instances:
      self.instances[handle].teardown()
      del self.instances[handle]
      del self.allmodules.instances[rval]
  def reset(self):             #writable extended attribute (setting to one results in reset)
    handles=self.instances.keys()
    for handle in handles:
      self.unregister(handle)
  def instance_count(self):   #read-only extended attribute
    return len(self.instances)
  def throttle_info(self):    #read-only extended attribute
    set_size=len(self.anycast)
    set_volume=self.box.anycast_set_volume(anycast)
    return (set_size,set_volume)
  def anycast_add(self,carvpath,router_state,mime_type,file_extension):
    jobno=self.lastjobno
    self.lastjobno += 1
    jobhandle = "J" + blake2b("J" + hex(jobno)[2:].zfill(16),digest_size=32,key=self.secret).hexdigest()
    self.anycast[jobhandle]=Job(jobhandle,self.name,carvpath,router_state,mime_type,file_extension)
    self.allmodules.path_state[carvpath]="anycast"
    self.allmodules.path_module[carvpath]=self.name
  def anycast_pop(self,sort_policy,select_policy="S"):
    if self.name != "loadbalance":
      best=self.box.anycast_best(self.name,anycast,sort_policy)
      self.allmodules[best]=self.anycast.pop(best)
      self.allmodules.path_state[carvpath]="pending"
    else:
      best=self.allmodules.selectmodule(select_policy).anycast_pop(sort_policy,select_policy)
      self.allmodules.path_state[carvpath]="migrating"
    self.allmodules.path_module[carvpath]=self.name
    return best

class ModulesState:
  def __init__(self):
    self.modules={}
    self.instances={}
    self.jobs={}
    self.newdata={}
    self.path_state={} 
    self.path_module={}
    random.seed()
    self.rumpelstiltskin=''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(0,32))
    self.re_select = re.compile(r'^[SVDWC]{1,5}$')
    self.re_sort = re.compile(r'^[RrOHDdWS]{1,6}$')    
  def __getitem__(self,key):
    if not key in self.modules:
      strongname = "M" + blake2b("M" +key,digest_size=32,key=self.secret).hexdigest()
      self.modules[key]=ModuleState(key,strongnamei,self)
    return self.modules[key]
  def getinstance(self,handle):
    if handle in self.instances:
      return self.instances[handle]
    return None
  def getjob(self,handle):
    if handle in self.jobs:
      return self.jobs[handle]
    return None
  def getnewdata(self,handle):
    if handle in self.newdata:
      return self.newdata[handle]
    return None
  def selectmodule(self,select_policy):
    moduleset=self.modules.keys()
    if len(moduleset) == 0:
      return None
    if len(moduleset) == 1:
      return moduleset[0]
    for letter in select_policy:
      moduleset=self.box.anycast_best_modules(self,moduleset,letter)
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
  def validSelectPolicy(self,pol):
    return bool(self.re_select.search(pol))
  def validSortPolicy(self,pol): 
    return bool(self.re_sort.search(pol))

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
class NoList:
  def getattr(self):
    return  defaultstat(STAT_MODE_DIR_NOLIST)
  def opendir(self):
    return -errno.EPERM
  def readlink(self):
    return -errno.EINVAL
class TopCtl:
  def __init__(self,fs):
    self.fs=fs
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class ModuleCtl:
  def __init__(self,ms,mod):
    self.ms=ms
    self.mod=mod
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class InstanceCtl:
  def __init__(self,instance):
    self.instance=instance
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class JobCtl:
  def __init__(self,job):
    self.job=job
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class NewDataCtl:
  def __init__(self,newdata):
    self.newdata=newdata
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class CarvPathFile:
  def __init__(self,carvpath,rep):
    pass
  def getattr(self):
    return  defaultstat(STAT_MODE_FILE_RO)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return -errno.EINVAL
class CarvPathLink:
  def __init__(self,link):
    self.link = "../" + link
  def getattr(self):
    return  defaultstat(STAT_MODE_LINK)
  def opendir(self):
    return -errno.ENOTDIR
  def readlink(self):
    return self.link

class MattockFS(fuse.Fuse):
    def __init__(self,dash_s_do,version,usage):
      super(MattockFS, self).__init__(version=version,usage=usage,dash_s_do=dash_s_do)
      self.core=MattockFSCore()
      self.ms=ModulesState()
      self.rep=Repository()
      self.box=CarvpathBox(self.rep)
      self.topdir=TopDir()
      self.nolistdir=NoList()
      self.topctl=TopCtl(self)
    def parsepath(self,path):
        if path == "/":
          return self.topdir
        tokens=path[1:].split("/")
        if len(tokens) > 3:
          return None
        if tokens[0] in ("data","module","instance","job","newdata","mattockfs.ctl"):
          if len(tokens) >2 and tokens[0] != "data":
            return None
          if len(tokens) == 1:
            if tokens[0] == "mattockfs.ctl":
              return self.topctl
            return self.nolistdir
          if tokens[0] == "data":
            lastpart=tokens[1].split(".")
            if len(lastpart) > 2:
              return None
            topcp=lastpart[0]
            if not self.box.validcarvpath(topcp):
              return None
            if len(tokens) == 2:
              if len(lastpart) == 2:
                return CarvPathFile(topcp,self.rep)
              if len(lastpart) > 2:
                  return None
              return self.nolistdir
            #must be 3 now
            lastpart=tokens[2].split(".")
            if len(lastpart) > 2: 
              return None
            link=self.box.flatten(topcp,lastpart[0])
            if link != None:
              return CarvPathLink(link)
            return None
          lastpart=tokens[1].split(".")
          if len(lastpart) !=2:
            return None
          handle = lastpart[0]
          extension = lastpart[1]
          if extension == "ctl":
            if tokens[0] == "module":
              if self.ms.validmodulename(handle):
                return ModuleCtl(self.ms,handle)
              return None
            if tokens[0] == "instance":
              if self.ms.validinstancecap(handle):
                return InstanceCtl(self.ms.instances[handle])
              return None
            if tokens[0] == "job":
              if self.ms.validjobcap(handle):
                return JobCtl(self.ms.jobs[handle])
              return None
            return None
          if extension == "dat" and  tokens[0] == "newdata" and self.ms.validnewdatacap(handle):
            return NewDataCtl(self.ms.newdata[handle])
          return None
        
    def getattr(self, path):
        node=self.parsepath(path)
        if node == None:
          return -errno.ENOENT
        return node.getattr()
    def opendir(self, path):
        node=self.parsepath(path)
        if node == None:
          return -errno.ENOENT
        return node.opendir()
    def readdir(self, path, offset):
        node=self.parsepath(path)
        if node == None:
          return -errno.ENOENT
        return node.readdir()
    def readlink(self,path):
        node=self.parsepath(path)
        if node == None:
          return -errno.ENOENT
        return node.readlink()
    def listxattr(self, path,huh):
      print "listxattr", path, huh
      if path == "/":
        return []
      tokens=path[1:].split("/")
      print tokens
      if len(tokens) == 1:
        if tokens[0] == "mattockfs.ctl":
          return ["user.throttle_info","user.full_archive"]
        if tokens[0] in ("data","module","instance","job","newdata"):
          return []
        else:
          return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job","newdata"):
        return -errno.ENOENT
      if tokens[0] == "data":
        if len(tokens) == 2:
          lastpart=tokens[1].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          if self.box.validcarvpath(lastpart[0]):
            if len(lastpart) == 1:
              return []
            return ["user.path_state","user.throttle_info"]
          return -errno.ENOENT
        if len(tokens) == 3:
          if not self.box.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.box.flatten(tokens[1],lastpart[0])
          if flattened != None:
            return []
          return -errno.ENOENT
        return -errno.ENOENT
      if len(tokens) > 2:
          return -errno.ENOENT
      lastpart=tokens[1].split(".")
      if len(lastpart) !=2:
        return -errno.ENOENT
      handle = lastpart[0]
      extension = lastpart[1]
      if tokens[0] == "module":
        if extension == "ctl" and self.ms.validmodulename(handle):
          return ["user.weight","user.overflow","user.throttle_info","user.instance_count","user.reset","user.register_instance"]
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.ms.validinstancecap(handle):
          return ["user.sort_policy","user.select_policy","user.unregister","user.active","accept_job"]
        return -errno.ENOENT
      if tokens[0] == "job":
        if extension == "ctl" and self.ms.validjobcap(handle):
          return ["user.routing_info","user.derive_child_entity","user.create_mutable_child_entity","user.set_child_submit_info","user.active_child","user.job_carvpath"]
        return -errno.ENOENT
      if tokens[0] == "newdata":
        if extension == "dat" and self.ms.validnewdatacap(handle):
          return []
        return -errno.ENOENT
      return -errno.ENOENT

    def getxattr(self, path, name, size):
      print "getxattr",path,name,size
      if path == "/":
        return -errno.ENODATA
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] == "mattockfs.ctl":
          if name == "user.throttle_info":
            return ";".join(map(lambda x: str(x),self.core.getTopThrottleInfo()))
          return -errno.ENODATA
        if tokens[0] in ("data","module","instance","job","newdata"):
          return -errno.ENODATA
        else:
          return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job","newdata"):
        return -errno.ENOENT
      if tokens[0] == "data":
        if len(tokens) == 2:
          lastpart=tokens[1].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          if self.box.validcarvpath(lastpart[0]):
            if len(lastpart) == 1:
              return -errno.ENODATA
            if name == "user.path_state":
              return self.core.getPathState(lastpart[0])
            if name == "user.throttle_info":
              return ";".join(map(lambda x: str(x),self.core.getPathThrottleInfo(handle)))
            else:
              return -errno.ENODATA
          return -errno.ENOENT
        if len(tokens) == 3:
          if not self.box.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.box.flatten(tokens[1],lastpart[0])
          if flattened != None:
            return -errno.ENODATA
          return -errno.ENOENT
        return -errno.ENOENT
      if len(tokens) > 2:
        return -errno.ENOENT
      lastpart=tokens[1].split(".")
      if len(lastpart) !=2:
        return -errno.ENOENT
      handle = lastpart[0]
      extension = lastpart[1]
      if tokens[0] == "module":
        if extension == "ctl" and self.ms.validmodulename(handle):
          if name == "user.weight":
            return str(self.ms[handle].weight)
          if name == "user.overflow":
            return str(self.ms[handle].overflow)
          if name == "user.throttle_info":
            return ";".join(map(lambda x: str(x),self.ms[handle].throttle_info()))
          return -errno.ENODATA 
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.ms.validinstancecap(handle):
          if name == "user.sort_policy":
            return self.core.getInstanceSortPollicy(handle)
          if name == "user.select_policy":
            return self.core.getInstanceSelectPollicy(handle)
          if name == "user.unregister":
            return "0"
      if tokens[0] == "job":
        if extension == "ctl" and self.ms.validjobcap(handle):
          if name == "user.routing_info":
            return self.ws.jobs[handle].routing_info
          if name == "user.set_child_submit_info":
            return ""
          if name == "user.derive_child_entity":
            return ""
          if name == "user.create_mutable_child_entity":
            return ""
          return -errno.ENODATA
        return  -errno.ENOENT
      if tokens[0] == "newdata":
        if extension == "dat" and self.ms.validnewdatacap(handle):
          return -errno.ENODATA
        return  -errno.ENOENT

    def setxattr(self, path, name, val, more):
      if path == "/":
        return -errno.ENODATA
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] == "mattockfs.ctl":
          if name == "throttle_info":
            return -errno.EPERM
          return -errno.ENODATA
        if tokens[0] in ("data","module","instance","job","newdata"):
          return -errno.ENODATA
        return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job","newdata"):
        return -errno.ENOENT
      if tokens[0] == "data":
        if len(tokens) == 2:
          lastpart=tokens[1].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          if self.box.validcarvpath(lastpart[0]):
            if len(lastpart) == 1:
              return -errno.ENODATA
            if name == "user.path_state":
              return -errno.EPERM
            if name == "user.throttle_info":
              return -errno.EPERM
            else:
              return -errno.ENODATA
          return -errno.ENOENT
        if len(tokens) == 3:
          if not self.box.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.box.flatten(tokens[1],lastpart[0])
          if flattened != None:
            return -errno.ENODATA
          return -errno.ENOENT
        return -errno.ENOENT
      if len(tokens) > 2:
          return -errno.ENOENT
      lastpart=tokens[1].split(".")
      if len(lastpart) !=2:
        return -errno.ENOENT
      handle = lastpart[0]
      extension = lastpart[1]
      if tokens[0] == "module":
        if extension == "ctl" and self.ms.validmodulename(handle):
          if name == "user.weight":
            ival=0
            try:
              ival = int(val)
            except:
              return -errno.EINVAL
            self.ms[lastpart[0]].weight=int(ival)
            return 0
          if name == "user.overflow":
            ival=0
            try:
              ival = int(val)
            except:
              return -errno.EINVAL
            self.ms[lastpart[0]].overflow=int(ival)
            return 0
          if name == "user.throttle_info":
            return -errno.EPERM
          return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.ms.validinstancecap(handle):
          if name == "user.sort_policy":
            if self.ms.validSortPolicy(val):
              self.core.setInstanceSortPollicy(handle,val)
            return 0
          if name == "user.select_policy":
            if self.ms.validSelectPolicy(val):
              self.core.setInstanceSelectPollicy(handle,val)
            return 0
          if name == "user.unregister":
            if val == "1":
              self.ms[handle].unregister(handle)
            return 0
          return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "job":
        if extension == "ctl" and self.ms.validjobcap(handle):
          if name == "user.routing_info":
            valtokens=val.split(";")
            if len(valtokens) !=2:
              return -errno.EINVAL
            if self.ms.validmodulename(valtokens[0]):
              self.ws.jobs[handle].routing_info = valtokens[1]
            return 0
          if name == "user.set_child_submit_info":
            valtokens=val.split(";")
            if len(valtokens) !=4:
              return -errno.EINVAL
            if self.ms.validmodulename(valtokens[0]) and self.core.validMimeType(valtokens[2]) and self.core.validExtension(valtokens[3]):
              self.ms.jobs[handle].submit_info=valtokens
              return 0
            else:
              return -errno.EINVAL
          if name == "user.derive_child_entity":
            if self.box.validcarvpath(val):
              self.core.deriveChildEntity(handle,val)
            return 0
          if name == "user.create_mutable_child_entity":
            ival=0
            try:
              ival = int(val)
            except:
              return 0
            self.core.createMutableChildEntity(handle,ival)
            return 0
          return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "newdata":
        if extension == "dat" and self.ms.validnewdatacap(handle):
          return -errno.ENODATA
        return -errno.ENOENT


if __name__ == '__main__':
    mattockfs = MattockFS(version = '%prog ' + '0.1.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle')
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()
