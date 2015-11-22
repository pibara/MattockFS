#!/usr/bin/python
import fuse
import stat
import errno 

fuse.fuse_python_api = (0, 2)

class MattockFSCore:
  def datacapsize(self,handle):
    return 18181874
  def getThottleInfo(self):
    return (8000000,0,900000000,1689990,12,19000)
  def validjobcap(self,cap):
    return True
  def validcarvpath(self,cp):
    return True
  def repositorySize(self):
    return 12345678
  def getJobCarvPath(self,jobcap):
    return "data/10000+23456"
  def getJobFileExtension(self,jobcap):
    return "gif"
  def FlattenCarvPath(self,basecp,subcp):
    return "8756+1744"
  def registerNewInstance(self,module):
    return "boo47lntpq91t"
  def unregisterInstance(self,handle):
    return
  def carvpathSize(self,cp):
    return 2345
  def validModuleName(self,modname):
    return True
  def validinstancecap(self,cap):
    return True
  def validnewdatacap(self,cap):
    return True
  def validSelectPolicy(self,pol):
    return True
  def validSortPolicy(self,pol):
    return True
  def acceptJob(self,instancecap):
    return "J47nPqWptT"
  def createSubJobFromCarvpath(self,jobcap,carvpath):
    return "Sub759BfFQ"
  def getPathState(self,cp):
    return "anycast;antiword;application/ms-word;doc;INCOMPLETE;4096"
  def getPathThottleInfo(self,cp): 
    return (8000000,0,900000000,1689990,12,19000)
  def getModuleWeight(self,module):
    return 100
  def getModuleOverflow(self,module):
    return 10
  def setModuleWeight(self,module,weight):
    return
  def setModuleOverflow(self,module,overflow):
    return
  def getModuleThrottleState(self,module):
    return (8000000,0,900000000,1689990,12,19000)
  def getJobRoutingInfo(self,jobcap):
    return "dsm;"
  def getJobSubmitInfo(self,jobcap):
    return "dsm;;application/octetstream;data"
  def setJobRoutingInfo(self,jobcap,nextmodule,routerstate):
    return
  def setJobSubmitInfo(self,jobcap,nextmodule,routerstate,mimetype,ext):
    return
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

def defaultstat():
  st = fuse.Stat()
  st.st_blksize= 512
  st.st_mode = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
  st.st_nlink = 1
  st.st_uid = 0
  st.st_gid = 0
  st.st_size = 0
  st.st_blocks = 0
  st.st_atime =  0
  st.st_mtime = 0
  st.st_ctime = 0
  return st

class MattockFS(fuse.Fuse):
    def __init__(self,dash_s_do,version,usage):
      super(MattockFS, self).__init__(version=version,usage=usage,dash_s_do=dash_s_do)
      self.core=MattockFSCore()
    def getattr(self, path):
        print "getattr" , path
        st = defaultstat()
        normaldirmode = stat.S_IFDIR |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
        nolistdirmode = stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        symlinkmode = stat.S_IFLNK |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
        regfilemode = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        rwfilemode = stat.S_IFREG |stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
        if path == "/":
          st.st_mode = normaldirmode
          return st
        tokens=path[1:].split("/")
        if tokens[0] in ("data","module","instance","job","newdata","mattockfs.ctl"):
          if len(tokens) == 1:
            if tokens[0] == "mattockfs.ctl":
              st.st_mode = symlinkmode
              return st
            st.st_mode = nolistdirmode
            return st
          if tokens[0] == "data":
            if len(tokens) == 2:
              lastpart=tokens[1].split(".")
              if len(lastpart) > 2:
                return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/ entries.
              if self.core.validcarvpath(lastpart[0]):
                if len(lastpart) == 2:
                  st.st_mode = regfilemode
                  st.st_size = self.core.carvpathSize(lastpart[0])
                  print "Returning regular file stat"
                  return st
                st.st_mode = nolistdirmode
                return st
              return -errno.ENOENT      #Invalid carvpath
            if len(tokens) == 3 and self.core.validcarvpath(tokens[1]):
              lastpart=tokens[2].split(".")
              if len(lastpart) > 2:
                return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/<carvpath>/
              if self.core.validcarvpath(lastpart[0]):
                st.st_mode = symlinkmode
                return st
              return -errno.ENOENT #Invalid carvpath
            return -errno.ENOENT #No entities that deep in the data dir.
          if len(tokens) > 2:
            return -errno.ENOENT #No entities that deep.
          lastpart=tokens[1].split(".")
          if len(lastpart) !=2:
            return -errno.ENOENT
          handle = lastpart[0]
          extension = lastpart[1]
          if tokens[0] == "module":
            if extension == "ctl" and self.core.validModuleName(handle):
              st.st_mode = symlinkmode
              return st
            return -errno.ENOENT
          if tokens[0] == "instance":
            if extension == "ctl" and self.core.validinstancecap(handle):
              st.st_mode = symlinkmode
              return st
            return -errno.ENOENT
          if tokens[0] == "job":
            if extension == "ctl" and self.core.validjobcap(handle):
              st.st_mode = symlinkmode
              return st
            return -errno.ENOENT
          if tokens[0] == "newdata":
            if extension == "dat" and self.core.validnewdatacap(handle):
              st.st_mode = rwfilemode
              st.st_size = self.core.datacapsize(handle)
              return st
            return -errno.ENOENT
        else:
          return -errno.ENOENT

    def opendir(self, path):
        if path == "/":
          return 0 #Root dir is a regular listable directory.
        tokens=path[1:].split("/")
        if not tokens[0] in ("data","module","instance","job","newdata","mattockfs.ctl"):
          return -errno.ENOENT
        if len(tokens) == 1:
          if tokens[0] == "mattockfs.ctl":
            return -errno.ENOTDIR   #$mp/mattockfs.ctl is a symlink not a dir.
          return -errno.EPERM #$mp/data , $mp/module, $mp/instance and $mp/job ar unlistable directories
        if tokens[0] == "data":
          if len(tokens) == 2:
            lastpart=tokens[1].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/ entries.
            if self.core.validcarvpath(lastpart[0]):
              if len(lastpart) == 2:
                return -errno.ENOTDIR #$mp/data/<carvpath>.<ext> is a file, not a dir
              return -errno.EPERM     #$mp/data/<carvpath> is an unlistable dir.
            return -errno.ENOENT      #Invalid carvpath
          if len(tokens) == 3 and self.core.validcarvpath(tokens[1]):
            lastpart=tokens[2].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/<carvpath>/
            if self.core.validcarvpath(lastpart[0]):
              return -errno.ENOTDIR #$mp/data/<carvpath>/<carvpath>[.<ext>] is a symlink, not a dir.
            return -errno.ENOENT #Invalid carvpath
          return -errno.ENOENT #No entities that deep in the data dir.
        if len(tokens) > 2:
            return -errno.ENOENT #No entities that deep.
        lastpart=tokens[1].split(".")
        if len(lastpart) !=2:
          return -errno.ENOENT
        handle = lastpart[0]
        extension = lastpart[1]
        if tokens[0] == "module":
          if extension == "ctl" and self.core.validModuleName(handle):
             return -errno.ENOTDIR
          return -errno.ENOENT
        if tokens[0] == "instance":
          if extension == "ctl" and self.core.validinstancecap(handle):
            return -errno.ENOTDIR
          return -errno.ENOENT
        if tokens[0] == "job": 
          if extension == "ctl" and self.core.validjobcap(handle):
            return -errno.ENOTDIR
          return -errno.ENOENT
        if tokens[0] == "newdata":
          if extension == "dat" and self.core.validnewdatacap(handle):
            return -errno.ENOTDIR
          return -errno.ENOENT
        return -errno.ENOENT

    def readdir(self, path, offset):
        if path == "/":
          yield fuse.Direntry("mattockfs.ctl")
          yield fuse.Direntry("data")
          yield fuse.Direntry("module")
          yield fuse.Direntry("instance")         
          yield fuse.Direntry("job")
          yield fuse.Direntry("newdata")

    def readlink(self,path):
        if path == "/":
          return -errno.EINVAL #root dir is no symlink
        tokens=path[1:].split("/")
        if len(tokens) == 1 and tokens[0] == "mattockfs.ctl":
          return "./data/0+" + str(self.core.repositorySize()) + ".dd"
        if len(tokens) == 1:
          if  tokens[0] in ("data","module","instance","job","newdata"):
            return -errno.EINVAL #dir not symlink
          return -errno.ENOENT
        if tokens[0] == "data":
          if len(tokens) == 2:
            lastpart=tokens[1].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT
            if self.core.validcarvpath(lastpart[0]):
              return -errno.EINVAL  #dir or file, not a symlink
            return -errno.ENOENT
          if len(tokens) == 3 and self.core.validcarvpath(tokens[1]):
            if not self.core.validcarvpath(tokens[1]):
              return -errno.ENOENT
            lastpart=tokens[2].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT
            ext=""
            if len(lastpart) == 2:
              ext = "." + lastpart[1]
            flattened=self.core.FlattenCarvPath(tokens[1],lastpart[0])
            if flattened != None:
              return "../" + flattened + ext
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
          if extension == "ctl" and self.core.validModuleName(handle):
            return "../instance/" + self.core.registerNewInstance(handle) + ".ctl"
          return -errno.ENOENT
        if tokens[0] == "instance":
          if extension == "ctl" and self.core.validinstancecap(handle):
            return "../job/" + self.core.acceptJob(handle) + ".ctl"
          return -errno.ENOENT
        if tokens[0] == "job":
          if extension == "ctl" and self.core.validjobcap(handle):
            return "../" + self.core.getJobCarvPath(handle)
          return -errno.ENOENT
        if tokens[0] == "newdata":
          if extension == "dat" and self.core.validnewdatacap(handle):
            return -errno.ENODATA
        return -errno.ENOENT      

    def listxattr(self, path,huh):
      print "listxattr", path, huh
      if path == "/":
        return []
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] == "mattockfs.ctl":
          return ["throttle\_info"]
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
          if self.core.validcarvpath(lastpart[0]):
            if len(lastpart) == 1:
              return []
            return ["user.path_state","throttle\_info"]
          return -errno.ENOENT
        if len(tokens) == 3:
          if not self.core.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.core.FlattenCarvPath(tokens[1],lastpart[0])
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
        if extension == "ctl" and self.core.validModuleName(handle):
          return ["user.weight","user.overflow","user.throttle_info"]
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.core.validinstancecap(handle):
          return ["user.sort_policy","user.select_policy","user.registered"]
        return -errno.ENOENT
      if tokens[0] == "job":
        if extension == "ctl" and self.core.validjobcap(handle):
          return ["user.routing_info","user.derive_child_entity","user.create_mutable_child_entity","user.last_child_submit_info"]
        return -errno.ENOENT
      if tokens[0] == "newdata":
        if extension == "dat" and self.core.validnewdatacap(handle):
          return []
        return -errno.ENOENT
      return -errno.ENOENT

    def getxattr(self, path, name, position=0):
      if path == "/":
        return -errno.ENODATA
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] == "mattockfs.ctl":
          if name == "throttle_info":
            return ";".join(map(lambda x: str(x),self.core.getThrottleInfo(handle)))
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
          if self.core.validcarvpath(lastpart[0]):
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
          if not self.core.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.core.FlattenCarvPath(tokens[1],lastpart[0])
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
        if extension == "ctl" and self.core.validModuleName(handle):
          if name == "user.weight":
            return str(self.core.getModuleWeight(handle))
          if name == "user.overflow":
            return str(self.core.getModuleOverflow(handle))
          if name == "user.throttle_info":
            return ";".join(map(lambda x: str(x),self.core.getModuleThrottleState(handle)))
          return -errno.ENODATA 
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.core.validinstancecap(handle):
          if name == "user.sort_policy":
            return self.core.getInstanceSortPollicy(handle)
          if name == "user.select_policy":
            return self.core.getInstanceSelectPollicy(handle)
          if name == "user.registered":
            return "1"
      if tokens[0] == "job":
        if extension == "ctl" and self.core.validjobcap(handle):
          if name == "user.routing_info":
            return self.core.getJobRoutingInfo(handle)  
          if name == "user.last_child_submit_info":
            return ""
          if name == "user.derive_child_entity":
            return ""
          if name == "user.create_mutable_child_entity":
            return ""
          return -errno.ENODATA
        return  -errno.ENOENT
      if tokens[0] == "newdata":
        if extension == "dat" and self.core.validnewdatacap(handle):
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
          if self.core.validcarvpath(lastpart[0]):
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
          if not self.core.validcarvpath(tokens[1]):
            return -errno.ENOENT
          lastpart=tokens[2].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          flattened=self.core.FlattenCarvPath(tokens[1],lastpart[0])
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
        if extension == "ctl" and self.core.validModuleName(handle):
          if name == "user.weight":
            ival=0
            try:
              ival = int(val)
            except:
              return -errno.EINVAL
            self.core.setModuleWeight(lastpart[0],ival)
            return 0
          if name == "user.overflow":
            ival=0
            try:
              ival = int(val)
            except:
              return -errno.EINVAL
            self.core.setModuleOverflow(lastpart[0],ival)
            return 0
          if name == "user.throttle_info":
            return -errno.EPERM
          return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "instance":
        if extension == "ctl" and self.core.validinstancecap(handle):
          if name == "user.sort_policy":
            if self.core.validSortPolicy(val):
              self.core.setInstanceSortPollicy(handle,val)
            return 0
          if name == "user.select_policy":
            if self.core.validSelectPolicy(val):
              self.core.setInstanceSelectPollicy(handle,val)
            return 0
          if name == "user.registered":
            if val == "0":
              self.core.unregisterInstance(handle)
            return 0
          return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "job":
        if extension == "ctl" and self.core.validjobcap(handle):
          if name == "user.routing_info":
            valtokens=val.split(";")
            if len(valtokens) !=2:
              return -errno.EINVAL
            if self.core.validModuleName(valtokens[0]):
              self.core.setJobRoutingInfo(valtokens[0],valtokens[1])
            return 0
          if name == "user.last_child_submit_info":
            valtokens=val.split(";")
            if len(valtokens) !=4:
              return -errno.EINVAL
            if self.core.validModuleName(valtokens[0]) and self.core.validMimeType(valtokens[2]) and self.core.validExtension(valtokens[3]):
              self.core.setJobSubmitInfo(valtokens[0],valtokens[1],valtokens[2],valtokens[3])
              return 0
            else:
              return -errno.EINVAL
          if name == "user.derive_child_entity":
            if self.core.validcarvpath(val):
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
        if extension == "dat" and self.core.validnewdatacap(handle):
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
