#!/usr/bin/python
import fuse
import stat
import errno 

fuse.fuse_python_api = (0, 2)

def validjobcap(cap):
  return True
def validcarvpath(cp):
  return True
def getRepositoryCarvPath():
  return "0+12345678"
def getJobCarvPath(jobcap):
  return "10000+23456"
def getJobFileExtension(jobcap):
  return "gif"
def FlattenCarvPath(basecp,subcp):
  return "8756+1744"
def registerNewInstance(module):
  return "boo47lntpq91t"
def carvpathSize(cp):
  return 2345
def validModuleName(modname):
  return True
def validinstancecap(cap):
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
    def getattr(self, path):
        print "getattr" , path
        st = defaultstat()
        normaldirmode = stat.S_IFDIR |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
        nolistdirmode = stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        symlinkmode = stat.S_IFLNK |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
        regfilemode = stat.S_IFREG |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
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
              if validcarvpath(lastpart[0]):
                if len(lastpart) == 2:
                  st.st_mode = regfilemode
                  st.st_size = carvpathSize(lastpart[0])
                  print "Returning regular file stat"
                  return st
                st.st_mode = nolistdirmode
                return st
              return -errno.ENOENT      #Invalid carvpath
            if len(tokens) == 3 and validcarvpath(tokens[1]):
              lastpart=tokens[2].split(".")
              if len(lastpart) > 2:
                return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/<carvpath>/
              if validcarvpath(lastpart[0]):
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
            if validModuleName(lastpart[0]):
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
            if not validinstancecap(tokens[1]):
              return -errno.ENOENT
            if len(tokens) == 2:
              st.st_mode = nolistdirmode
              return st
            if len(tokens) == 3:
              if tokens[2] == "instance.ctl":
                st.st_mode = regfilemode
                return st
              lastpart=lastpart=tokens[2].split(".")
              if len(lastpart) != 2 or lastpart[1] != "accept":
                return -errno.ENOENT
              lp2=lastpart[0].split("-")
              if len(lp2) != 2:
                return -errno.ENOENT
              if validSelectPolicy(lp2[0]) and validSortPolicy(lp2[1]):
                st.st_mode = symlinkmode
                return st
              return -errno.ENOENT #No other valid entities at this level.
            return -errno.ENOENT
          if tokens[0] == "job":
            if not validjobcap(tokens[1]):
              return -errno.ENOENT
            if len(tokens) == 2:
              return 0 ##$mp/job/<jobcap>/ is e regular listable directory.
            if len(tokens) == 3 and  tokens[2] in ("data","newdata"):
              st.st_mode = nolistdirmode
              return st
            if len(tokens) == 4:
              lastpart=tokens[3].split(".")
              if len(lastpart) != 2:
                return -errno.ENOENT
              if not validcarvpath(lastpart[0]):
               return -errno.ENOENT
              if tokens[2] == "data":
                st.st_mode = symlinkmode
                return st
              if tokens[2] == "newdata":
                if lastpart[0][:2] == "0+":
                  st.st_mode = regfilemode
                  st.st_size = carvpathSize(lastpart[0])
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
        if tokens[0] in ("data","module","instance","job","data.all"):
          if len(tokens) == 1:
            if tokens[0] == "data.all":
              return -errno.ENOTDIR   #$mp/data.all is a symlink not a dir.
            return -errno.EPERM #$mp/data , $mp/module, $mp/instance and $mp/job ar unlistable directories
          if tokens[0] == "data":
            if len(tokens) == 2:
              lastpart=tokens[1].split(".")
              if len(lastpart) > 2:
                return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/ entries.
              if validcarvpath(lastpart[0]):
                if len(lastpart) == 2:
                  return -errno.ENOTDIR #$mp/data/<carvpath>.<ext> is a file, not a dir
                return -errno.EPERM     #$mp/data/<carvpath> is an unlistable dir.
              return -errno.ENOENT      #Invalid carvpath
            if len(tokens) == 3 and validcarvpath(tokens[1]):
              lastpart=tokens[2].split(".")
              if len(lastpart) > 2:
                return -errno.ENOENT #More than one dot in filename, not valid for $mp/data/<carvpath>/
              if validcarvpath(lastpart[0]):
                return -errno.ENOTDIR #$mp/data/<carvpath>/<carvpath>[.<ext>] is a symlink, not a dir.
              return -errno.ENOENT #Invalid carvpath
            return -errno.ENOENT #No entities that deep in the data dir.
          if tokens[0] == "module":
            if len(tokens) > 2:
              return -errno.ENOENT #No entities that deep in the module dir.
            lastpart=tokens[1].split(".")
            if len(lastpart !=2):
              return -errno.ENOENT
            if validModuleName(lastpart[0]):
              if lastpart[1] in ("register","ctl"):
                return -errno.ENOTDIR #Entities are symlink or file, not directories.
              else:
                return -errno.ENOENT #Other file extensions don't exist here.
            return -errno.ENOENT #Invalid volume name.
          if tokens[0] == "instance":
            if not validinstancecap(tokens[1]):
              return -errno.ENOENT
            if len(tokens) == 2:
              return -errno.EPERM     #$mp/instance/<moduleinstance>/ is an unlistable dir.
            if len(tokens) == 3:
              if tokens[2] == "instance.ctl":
                return -errno.ENOTDIR #$mp/instance/<moduleinstance>/instance.ctl is a file
              lastpart=lastpart=tokens[2].split(".")
              if len(lastpart) != 2 or lastpart[1] != "accept":
                return -errno.ENOENT
              lp2=lastpart[0].split("-")
              if len(lp2) != 2:
                return -errno.ENOENT 
              if validSelectPolicy(lp2[0]) and validSortPolicy(lp2[1]):
                return -errno.ENOTDIR #$mp/instance/<moduleinstance>/<select-policy>-<sort-policy>.accept is a symlink
              return -errno.ENOENT #No other valid entities at this level.
            return -errno.ENOENT
          if tokens[0] == "job": 
            if not validjobcap(tokens[1]):
              return -errno.ENOENT
            if len(tokens) == 2:
              return 0 ##$mp/job/<jobcap>/ is e regular listable directory.
            if len(tokens) == 3 and  tokens[2] in ("data","newdata"):
              return -errno.EPERM #$mp/job/<jobcap>/data/ and $mp/job/<jobcap>/newdata/ are non-listable directories.
            if len(tokens) == 4:
              lastpart=tokens[3].split(".")
              if len(lastpart) != 2:
                return -errno.ENOENT
              if not validcarvpath(lastpart[0]):
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
            if len(tokens) == 2 and validjobcap(tokens[1]):
              yield fuse.Direntry("data.all")
              yield fuse.Direntry("data")
              yield fuse.Direntry("newdata")
          print tokens

    def readlink(self,path):
        tokens=path[1:].split("/")
        if len(tokens) == 1 and tokens[0] == "data.all":
          return "./data/" + getRepositoryCarvPath() + ".dd"
        if tokens[0] == "data":
          if len(tokens) == 2:
            lastpart=tokens[1].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT
            if validcarvpath(lastpart[0]):
              return -errno.EPERM
          if len(tokens) == 3 and validcarvpath(tokens[1]):
            lastpart=tokens[2].split(".")
            if len(lastpart) > 2:
              return -errno.ENOENT
            ext=""
            if len(lastpart) == 2:
              ext = "." + lastpart[1]
            flattened=FlattenCarvPath(tokens[1],lastpart[0])
            if flattened != None:
              return "../" + flattened + ext
            else:
              return -errno.ENOENT
        if len(tokens) == 2 and tokens[0] == "module":
          lastpart=tokens[1].split(".")
          if len(lastpart) !=2:
            return -errno.ENOENT
          if validModuleName(lastpart[0]):
            if lastpart[1] == "register":
              return "../instance/" + registerNewInstance(lastpart[0])
            if lastpart[1] == "ctl":
              return -errno.EPERM
        if len(tokens) == 3 and tokens[0] == "job" and validjobcap(tokens[1]) and tokens[2] == "data.all":
          return "./data/" + getJobCarvPath(tokens[1]) + "." + getJobFileExtension(tokens[1])
        return -errno.ENOENT


if __name__ == '__main__':
    mattockfs = MattockFS(version = '%prog ' + '0.1.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle')
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()
