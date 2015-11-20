#!/usr/bin/python
import fuse
import stat
import errno 

fuse.fuse_python_api = (0, 2)

def validjobcap(cap):
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
        print path
        st = defaultstat()
        if "." in path:
          st.st_mode = stat.S_IFLNK |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
        else: 
          st.st_mode = stat.S_IFDIR |stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH 
        return st

    def opendir(self, path):
        if path == "/":
          return 0
        tokens=path[1:].split("/")
        if tokens[0] in ("data","module","instance","job"):
          if len(tokens) == 1:
            return -errno.EPERM
          if tokens[0] == "job" and validjobcap(tokens[1]):
            if len(tokens) == 2:
              return 0
            if len(tokens) == 3 and  tokens[2] in ("data","newdata"):
              return -errno.EPERM
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
          return "./data/S0.dd"
        if len(tokens) == 3 and tokens[0] == "job" and validjobcap(tokens[1]) and tokens[2] == "data.all":
          return "./data/S0.dd"
        return -errno.ENOENT


if __name__ == '__main__':
    mattockfs = MattockFS(version = '%prog ' + '0.1.0',
               usage = 'Mattock filesystem ' + fuse.Fuse.fusage,
               dash_s_do = 'setsingle')
    mattockfs.parse(errex = 1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    mattockfs.main()
