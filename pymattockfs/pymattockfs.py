#!/usr/bin/python
import fuse
import stat
import errno 

fuse.fuse_python_api = (0, 2)

class MattockFSCore:
  def validjobcap(self,cap):
    return True
  def validcarvpath(self,cp):
    return True
  def repositorySize(self):
    return 12345678
  def getJobCarvPath(self,jobcap):
    return "10000+23456"
  def getJobFileExtension(self,jobcap):
    return "gif"
  def FlattenCarvPath(self,basecp,subcp):
    return "8756+1744"
  def registerNewInstance(self,module):
    return "boo47lntpq91t"
  def carvpathSize(self,cp):
    return 2345
  def validModuleName(self,modname):
    return True
  def validinstancecap(self,cap):
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
        if tokens[0] in ("data","module","instance","job","data.all"):
          if len(tokens) == 1:
            if tokens[0] == "data.all":
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
          if tokens[0] == "module":
            if len(tokens) > 2:
              return -errno.ENOENT #No entities that deep in the module dir.
            lastpart=tokens[1].split(".")
            if len(lastpart) !=2:
              return -errno.ENOENT
            if self.core.validModuleName(lastpart[0]):
              if lastpart[1] =="register":
                st.st_mode = symlinkmode
                return st
              if lastpart[1] == "ctl":
                st.st_mode = regfilemode
                return st
              else:
                return -errno.ENOENT #Other file extensions don't exist here.
            return -errno.ENOENT #Invalid volume name.
          if tokens[0] == "instance":
            if not self.core.validinstancecap(tokens[1]):
              return -errno.ENOENT
            if len(tokens) == 2:
              st.st_mode = nolistdirmode
              return st
            if len(tokens) == 3:
              lastpart=lastpart=tokens[2].split(".")
              if len(lastpart) != 2 or lastpart[1] != "accept":
                return -errno.ENOENT
              lp2=lastpart[0].split("-")
              if len(lp2) != 2:
                return -errno.ENOENT
              if self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
                st.st_mode = symlinkmode
                return st
              return -errno.ENOENT #No other valid entities at this level.
            return -errno.ENOENT
          if tokens[0] == "job":
            if not self.core.validjobcap(tokens[1]):
              print "Invalid jobcap"
              return -errno.ENOENT
            if len(tokens) == 2:
              st.st_mode = normaldirmode
              return st
            if len(tokens) == 3:
              if tokens[2] in ("data","newdata"):
                st.st_mode = nolistdirmode
                return st
              if tokens[2] == "data.all":
                st.st_mode = symlinkmode
                return st
              if tokens[2] == "job.ctl":
                st.st_mode = regfilemode
                return st
              return -errno.ENOENT
            if len(tokens) == 4:
              if not tokens[2] in ("data","newdata"):
                return -errno.ENOENT
              lastpart=tokens[3].split(".")
              if len(lastpart) != 2:
                return -errno.ENOENT
              if not self.core.validcarvpath(lastpart[0]):
               return -errno.ENOENT
              if tokens[2] == "data":
                st.st_mode = symlinkmode
                return st
              if tokens[2] == "newdata":
                if lastpart[0][:2] == "0+":
                  st.st_mode = rwfilemode
                  st.st_size = self.core.carvpathSize(lastpart[0])
                  return st
                return -errno.ENOENT
              return -errno.ENOENT
            return -errno.ENOENT
        else:
          return -errno.ENOENT

    def opendir(self, path):
        if path == "/":
          return 0 #Root dir is a regular listable directory.
        tokens=path[1:].split("/")
        if not tokens[0] in ("data","module","instance","job","data.all"):
          return -errno.ENOENT
        if len(tokens) == 1:
          if tokens[0] == "data.all":
            return -errno.ENOTDIR   #$mp/data.all is a symlink not a dir.
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
        if tokens[0] == "module":
          if len(tokens) > 2:
            return -errno.ENOENT #No entities that deep in the module dir.
          lastpart=tokens[1].split(".")
          if len(lastpart !=2):
            return -errno.ENOENT
          if self.core.validModuleName(lastpart[0]):
            if lastpart[1] in ("register","ctl"):
              return -errno.ENOTDIR #Entities are symlink or file, not directories.
            else:
              return -errno.ENOENT #Other file extensions don't exist here.
          return -errno.ENOENT #Invalid volume name.
        if tokens[0] == "instance":
          if not self.core.validinstancecap(tokens[1]):
            return -errno.ENOENT
          if len(tokens) == 2:
            return -errno.EPERM     #$mp/instance/<moduleinstance>/ is an unlistable dir.
          if len(tokens) == 3:
            lastpart=lastpart=tokens[2].split(".")
            if len(lastpart) != 2 or lastpart[1] != "accept":
              return -errno.ENOENT
            lp2=lastpart[0].split("-")
            if len(lp2) != 2:
              return -errno.ENOENT 
            if self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
              return -errno.ENOTDIR #$mp/instance/<moduleinstance>/<select-policy>-<sort-policy>.accept is a symlink
            return -errno.ENOENT #No other valid entities at this level.
          return -errno.ENOENT
        if tokens[0] == "job": 
          if not self.core.validjobcap(tokens[1]):
            return -errno.ENOENT
          if len(tokens) == 2:
            return 0 ##$mp/job/<jobcap>/ is e regular listable directory.
          if len(tokens) == 3: 
            if tokens[2] in ("data","newdata"):
              return -errno.EPERM #$mp/job/<jobcap>/data/ and $mp/job/<jobcap>/newdata/ are non-listable directories.
            if tokens[2] in ("data.all","job.ctl"):
              return -errno.ENOTDIR
            return -errno.ENOENT
          if len(tokens) == 4:
            if not tokens[2] in ("data","newdata"):
              return -errno.ENOENT
            lastpart=tokens[3].split(".")
            if len(lastpart) != 2:
              return -errno.ENOENT
            if not self.core.validcarvpath(lastpart[0]):
              return -errno.ENOENT
            if tokens[2] == "data":
              return -errno.ENOTDIR
            if tokens[2] == "newdata":
              if lastpart[0][:2] == "0+":
                return -errno.ENOTDIR
              return -errno.ENOENT
            return -errno.ENOENT
          return -errno.ENOENT
        return -errno.ENOENT

    def readdir(self, path, offset):
        if path == "/":
          yield fuse.Direntry("data.all")
          yield fuse.Direntry("data")
          yield fuse.Direntry("module")
          yield fuse.Direntry("instance")         
          yield fuse.Direntry("job")
        else:
          tokens=path[1:].split("/")
          if tokens[0] == "job":
            if len(tokens) == 2 and self.core.validjobcap(tokens[1]):
              yield fuse.Direntry("job.ctl")
              yield fuse.Direntry("data.all")
              yield fuse.Direntry("data")
              yield fuse.Direntry("newdata")
          print tokens

    def readlink(self,path):
        if path == "/":
          return -errno.EINVAL #root dir is no symlink
        tokens=path[1:].split("/")
        if len(tokens) == 1 and tokens[0] == "data.all":
          return "./data/0+" + str(repositorySize()) + ".dd"
        if len(tokens) == 1 and tokens[0] in ("data","module","instance","job"):
          return -errno.EINVAL #dir not symlink
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
        if tokens[0] == "module":
          if len(tokens) != 2:
            return -errno.ENOENT
          lastpart=tokens[1].split(".")
          if len(lastpart) !=2:
            return -errno.ENOENT
          if self.core.validModuleName(lastpart[0]):
            if lastpart[1] == "register":
              return "../instance/" + self.core.registerNewInstance(lastpart[0])
            if lastpart[1] == "ctl":
              return -errno.EINVAL  #file, not a symlink
            return -errno.ENOENT
          return -errno.ENOENT
        if tokens[0] == "instance":
          if not self.core.validinstancecap(tokens[1]):
              return -errno.ENOENT
          if len(tokens) == 2:
            return -errno.EINVAL  #dir not a symlink
          if len(tokens) == 3:
            lastpart=tokens[2].split(".")
            if len(lastpart) != 2 or lastpart[1] != "accept":
              return -errno.ENOENT
            lp2=lastpart[0].split("-")
            if len(lp2) == 2 and self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
              return "../../job/" + self.core.acceptJob(tokens[1])
          return -errno.ENOENT
        if tokens[0] == "job":
          if not self.core.validjobcap(tokens[1]):
            return -errno.ENOENT
          if len(tokens) == 2:
            return -errno.EINVAL  #dir not a symlink
          if len(tokens) == 3:
            if tokens[2] == "data.all":
              return "./data/" + self.core.getJobCarvPath(tokens[1]) + "." + self.core.getJobFileExtension(tokens[1])
            if tokens[2] == "job.ctl":
              return -errno.EINVAL  #file, not a symlink
            if tokens[2] in ("data","newdata"):
              return -errno.EINVAL  #dir not a symlink
            return  -errno.ENOENT
          if len(tokens) == 4:
            if not tokens[2] in ("data","newdata"):
              return -errno.ENOENT
            lastpart=tokens[3].split(".")
            if len(lastpart) != 2:
              return -errno.ENOENT
            if not self.core.validcarvpath(lastpart[0]):
              return -errno.ENOENT
            if tokens[2] == "data":
              return "../../../job/" + self.core.createSubJobFromCarvpath(tokens[1],lastpart[0])
            if tokens[2] == "newdata":
              if lastpart[0][:2] == "0+":
                return -errno.EINVAL  #file, not a symlink
              return -errno.ENOENT
            return -errno.ENOENT
          return -errno.ENOENT
        return -errno.ENOENT      

    def listxattr(self, path,huh):
      print "listxattr", path, huh
      if path == "/":
        return []
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] in ("data.all","data","module","instance","job"):
          return []
        else:
          return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job"):
        return -errno.ENOENT
      if tokens[0] == "data":
        if len(tokens) == 2:
          lastpart=tokens[1].split(".")
          if len(lastpart) > 2:
            return -errno.ENOENT
          if self.core.validcarvpath(lastpart[0]):
            if len(lastpart) == 1:
              return []
            print "Returning path_state"
            return ["user.path_state"]
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
      if tokens[0] == "module":
        if len(tokens) != 2:
          return -errno.ENOENT
        lastpart=tokens[1].split(".")
        if len(lastpart) !=2:
          return -errno.ENOENT
        if self.core.validModuleName(lastpart[0]):
          if lastpart[1] == "register":
            return []
          if lastpart[1] == "ctl":
            return ["user.weight","user.overflow","user.throttle_state"]
          return -errno.ENOENT
        return -errno.ENOENT
      if tokens[0] == "instance":
        if not self.core.validinstancecap(tokens[1]):
            return -errno.ENOENT
        if len(tokens) == 2:
          return []
        if len(tokens) == 3:
          lastpart=tokens[2].split(".")
          if len(lastpart) != 2 or lastpart[1] != "accept":
            return []
          lp2=lastpart[0].split("-")
          if len(lp2) == 2 and self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
            return []
        return -errno.ENOENT
      if tokens[0] == "job":
        if not self.core.validjobcap(tokens[1]):
          return -errno.ENOENT
        if len(tokens) == 2:
          return []
        if len(tokens) == 3:
          if tokens[2] == "data.all":
            return []
          if tokens[2] == "job.ctl":
            return ["user.routing_info","user.submit_info"]
          if tokens[2] in ("data","newdata"):
            return []
          return  -errno.ENOENT
        if len(tokens) == 4:
          if not tokens[2] in ("data","newdata"):
            return -errno.ENOENT
          lastpart=tokens[3].split(".")
          if len(lastpart) != 2:
            return -errno.ENOENT
          if not self.core.validcarvpath(lastpart[0]):
            return -errno.ENOENT
          if tokens[2] == "data":
            return []
          if tokens[2] == "newdata":
            if lastpart[0][:2] == "0+":
              return ["user.as_job"]
            return -errno.ENOENT
          return -errno.ENOENT 

    def getxattr(self, path, name, position=0):
      print "Seeking xattr ",name, " for ", path
      if path == "/":
        return -errno.ENODATA
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] in ("data.all","data","module","instance","job"):
          return -errno.ENODATA
        else:
          return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job"):
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
      if tokens[0] == "module":
        if len(tokens) != 2:
          return -errno.ENOENT
        lastpart=tokens[1].split(".")
        if len(lastpart) !=2:
          return -errno.ENOENT
        if self.core.validModuleName(lastpart[0]):
          if lastpart[1] == "register":
            return -errno.ENODATA
          if lastpart[1] == "ctl":
            if name == "user.weight":
              return str(self.core.getModuleWeight(lastpart[0]))
            if name == "user.overflow":
              return str(self.core.getModuleOverflow(lastpart[0]))
            if name == "user.throttle_state":
              return ";".join(map(lambda x: str(x),self.core.getModuleThrottleState(lastpart[0])))
            return -errno.ENODATA 
          return -errno.ENOENT
        return -errno.ENOENT
      if tokens[0] == "instance":
        if not self.core.validinstancecap(tokens[1]):
            return -errno.ENOENT
        if len(tokens) == 2:
          return -errno.ENODATA
        if len(tokens) == 3:
          lastpart=tokens[2].split(".")
          if len(lastpart) != 2 or lastpart[1] != "accept":
            return -errno.ENODATA
          lp2=lastpart[0].split("-")
          if len(lp2) == 2 and self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
            return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "job":
        if not self.core.validjobcap(tokens[1]):
          return -errno.ENOENT
        if len(tokens) == 2:
          return -errno.ENODATA
        if len(tokens) == 3:
          if tokens[2] == "data.all":
            return -errno.ENODATA
          if tokens[2] == "job.ctl":
            if name == "user.routing_info":
              return self.core.getJobRoutingInfo(tokens[1])  
            if name == "user.submit_info":
              return self.core.getJobSubmitInfo(tokens[1])
            return -errno.ENODATA
          if tokens[2] in ("data","newdata"):
            return -errno.ENODATA
          return  -errno.ENOENT
        if len(tokens) == 4:
          if not tokens[2] in ("data","newdata"):
            return -errno.ENOENT
          lastpart=tokens[3].split(".")
          if len(lastpart) != 2:
            return -errno.ENOENT
          if not self.core.validcarvpath(lastpart[0]):
            return -errno.ENOENT
          if tokens[2] == "data":
            return -errno.ENODATA
          if tokens[2] == "newdata":
            if lastpart[0][:2] == "0+":
              if name == "user.as_job":
                return "../../" + self.core.createSubJobFromMutable(tokens[1],lastpart[0])
              return -errno.ENODATA
            return -errno.ENOENT
          return -errno.ENOENT

    def setxattr(self, path, name, val, more):
      print "setxattr:",path, name, val, more
      if path == "/":
        return -errno.ENODATA
      tokens=path[1:].split("/")
      if len(tokens) == 1:
        if tokens[0] in ("data.all","data","module","instance","job"):
          return -errno.ENODATA
        else:
          return -errno.ENOENT
      if not tokens[0] in ("data","module","instance","job"):
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
      if tokens[0] == "module":
        if len(tokens) != 2:
          return -errno.ENOENT
        lastpart=tokens[1].split(".")
        if len(lastpart) !=2:
          return -errno.ENOENT
        if self.core.validModuleName(lastpart[0]):
          if lastpart[1] == "register":
            return -errno.ENODATA
          if lastpart[1] == "ctl":
            if name == "user.weight":
              ival=0
              try:
                ival = int(val)
              except:
                return -errno.EINVAL
              self.core.setModuleWeight(lastpart[0]),ival)
              return 0
            if name == "user.overflow":
              ival=0
              try:
                ival = int(val)
              except:
                return -errno.EINVAL
              self.core.setModuleOverflow(lastpart[0],ival)
              return 0
            if name == "user.throttle_state":
              return -errno.EPERM
            return -errno.ENODATA
          return -errno.ENOENT
        return -errno.ENOENT
      if tokens[0] == "instance":
        if not self.core.validinstancecap(tokens[1]):
            return -errno.ENOENT
        if len(tokens) == 2:
          return -errno.ENODATA
        if len(tokens) == 3:
          lastpart=tokens[2].split(".")
          if len(lastpart) != 2 or lastpart[1] != "accept":
            return -errno.ENODATA
          lp2=lastpart[0].split("-")
          if len(lp2) == 2 and self.core.validSelectPolicy(lp2[0]) and self.core.validSortPolicy(lp2[1]):
            return -errno.ENODATA
        return -errno.ENOENT
      if tokens[0] == "job":
        if not self.core.validjobcap(tokens[1]):
          return -errno.ENOENT
        if len(tokens) == 2:
          return -errno.ENODATA
        if len(tokens) == 3:
          if tokens[2] == "data.all":
            return -errno.ENODATA
          if tokens[2] == "job.ctl":
            if name == "user.routing_info":
              valtokens=val.split(";")
              if len(valtokens) !=2:
                return -errno.EINVAL
              if self.core.validModuleName(valtokens[0]):
                self.core.setJobRoutingInfo(valtokens[0],valtokens[1])
              return 0
            if name == "user.submit_info":
              valtokens=val.split(";")
              if len(valtokens) !=4:
                return -errno.EINVAL
              if self.core.validModuleName(valtokens[0]) and self.core.validMimeType(valtokens[2]) and self.core.validExtension(valtokens[3]):
                self.core.setJobSubmitInfo(valtokens[0],valtokens[1],valtokens[2],valtokens[3])
                return 0
              else:
                return -errno.EINVAL
            return -errno.ENODATA
          if tokens[2] in ("data","newdata"):
            return -errno.ENODATA
          return  -errno.ENOENT
        if len(tokens) == 4:
          if not tokens[2] in ("data","newdata"):
            return -errno.ENOENT
          lastpart=tokens[3].split(".")
          if len(lastpart) != 2:
            return -errno.ENOENT
          if not self.core.validcarvpath(lastpart[0]):
            return -errno.ENOENT
          if tokens[2] == "data":
            return -errno.ENODATA
          if tokens[2] == "newdata":
            if lastpart[0][:2] == "0+":
              if name == "user.as_job":
                return -errno.EPERM
              return -errno.ENODATA
            return -errno.ENOENT
          return -errno.ENOENT



if __name__ == '__main__':
    mattockfs = MattockFS(version = '%prog ' + '0.1.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle')
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()
