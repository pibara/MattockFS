#!/usr/bin/python3
import llfuse
import errno
import stat
from argparse import ArgumentParser

class Generate:
  def __init__(self):
    self.inode=0
  def __call__(self):
    self.inode += 1
    return self.inode

generate=Generate()

class Inode:
  def __init__(self,inode_no=generate(),size=4096,mode=stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH,nlink=1):
    print("Inode init")
    Inode.inode=inode_no
    Inode.entry = llfuse.EntryAttributes()
    Inode.entry.st_ino = inode_no
    Inode.entry.st_mode=mode
    Inode.entry.st_nlink=nlink 
    Inode.entry.generation = 0
    Inode.entry.entry_timeout = 30
    Inode.entry.attr_timeout = 30
    Inode.entry.st_uid = 0
    Inode.entry.st_gid = 0
    Inode.entry.st_rdev = 0
    Inode.entry.st_size = size
    Inode.entry.st_blksize = 512
    Inode.entry.st_blocks = 1
    Inode.entry.st_atime_ns = 0
    Inode.entry.st_mtime_ns = 0
    Inode.entry.st_ctime_ns = 0
  def getattr(self):
    print("Inode::getattr",self.inode)
    return Inode.entry
  def getxattr(self,name):
    print("getxattr",self.inode, name)
    return b''
  def setxattr(self,name, value):
    print("setxattr",self.inode, name, value)
    raise llfuse.FUSEError(errno.EPERM)
  def removexattr(self,name):
    print("removexattr",self.inode, name)
    raise llfuse.FUSEError(errno.EPERM)
  def lookup(self,name):
    print("lookup",self.inode,parent_inode, name)
    raise llfuse.FUSEError(errno.ENOENT)
  def listxattr(self):
    print("listxattr",self.inode)
    return []
  def readlink(self):
    print("readlink",self.inode)
    raise llfuse.FUSEError(errno.EPERM)
  def opendir(self):
    print("opendir",self.inode)
    raise llfuse.FUSEError(errno.EPERM)
  def readdir(self):
    print("readdir",self.inode)
    raise llfuse.FUSEError(errno.EPERM)
  def forget(self,inode_list):
    print("forget",inode_list)
    raise llfuse.FUSEError(errno.EPERM)
  def flush(self,fh):
    print("flush",inode,name)
    raise llfuse.FUSEError(errno.EPERM)
  def unlink(self, inode_p, name):
    print("unlink",inode,name)
    raise llfuse.FUSEError(errno.EPERM)
  def rmdir(self, inode_p, name):
    print("rmdir",inode_p,name)
    raise llfuse.FUSEError(errno.EPERM)
  def symlink(self, inode_p, name, target, ctx):
    print("symlink",inode_p,name,target,ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def rename(self, inode_p_old, name_old, inode_p_new, name_new):
    print("rename",inode_p_old,name_old, inode_p_new, name_new)
    raise llfuse.FUSEError(errno.EPERM)
  def link(self, inode, new_inode_p, new_name):
    print("link",inode, new_inode_p, new_name)
    raise llfuse.FUSEError(errno.EPERM)
  def setattr(self, inode, attr):
    print("setattr",inode, attr)
    raise llfuse.FUSEError(errno.EPERM)
  def mknod(self, inode_p, name, mode, rdev, ctx):
    print("mknod",inode_p, name, mode, rdev, ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def mkdir(self, inode_p, name, mode, ctx):
    print("mkdir",inode_p, name, mode, ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def statfs(self):
    print("statfs")
    return llfuse.StatvfsData()
  def open(self, inode, flags):
    print("open",inode, flags)
    raise llfuse.FUSEError(errno.EPERM)
  def access(self, inode, mode, ctx):
    print("access",inode, mode, ctx)
    return True
  def create(self, inode_parent, name, mode, flags, ctx):
    print("create",inode_parent, name, mode, flags, ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def read(self, fh, offset, length):
    print("read",fh, offset, length)
    raise llfuse.FUSEError(errno.EPERM)
  def write(self, fh, offset, buf):
    print("write",fh, offset, buf)
    raise llfuse.FUSEError(errno.EPERM)
  def release(self, fh):
    print("release",fh)
      
class RegularDir(Inode):
  def __init__(self,entries):
    super().__init__(mode=stat.S_IFDIR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH,nlink=42)
    self.entries=entries
  def opendir(self):
    print("opendir",self.inode)
    return self.inode
  def readdir(self):
    print("readdir",self.inode)
    pass
  def closedir():
    print("closedir",self.inode)
  def lookup(self,name):
    if name in self.entries:
      raise llfuse.FUSEError(errno.EPERM)
    else:
      raise llfuse.FUSEError(errno.ENOENT) 
      
class UnlistableDir(Inode):
  def __init__(self):
    super().__init__(mode=stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,nlink=3)
    

class SymLink(Inode):
    def __init__(self,link):
      self.link=link
      self.name=b'data.all'
    def readlink(self):
      return self.link

#class ReadOnlyFile:
#    def __init__(self,size):
#        self.size=size

#class AttributesFile(ReadOnlyFile):
#    def __init__(self):
#        super(ReadOnlyFile, self).__init__(0)

class RootDir(RegularDir):
    def __init__(self):
        super().__init__(entries=[b'data.all'])
    def readdir(self):
        yield TopLink()
       
class TopLink(SymLink):
    def __init__(self):
      super().__init__(b'./data/0+987000.dd')

#class CpTopDir(UnlistableDir):

#class CpEnt(ReadOnlyFile):

#class CpSubDir(UnlistableDir):

#class CpFlattenLink(Symlink):

#class ModuleTopDir(UnlistableDir):

#class RegisterLink(Symlink):

#class ModuleCtl(AttributesFile):

#class InstanceTopDir(UnlistableDir):

#class InstanceDir(UnlistableDir):

#class AcceptLink(Symlink):

#class InstanceCtl(AttributesFile):

#class JobTopDir(UnlistableDir):

#class JobTopLink(Symlink):

#class JobDataDir(UnlistableDir):

#class SubjobLink(Symlink):

#class JobnewDir(UnlistableDir):

#class JobMutable(RWFile):


class Operations(llfuse.Operations):
  def __init__(self):
        super(Operations, self).__init__()
        self.rootdir=RootDir()
        self.inodemap={}
        self.inodemap[1]=self.rootdir
  def getattr(self, inode):
    if inode in self.inodemap:
      return self.inodemap[inode].getattr()
    raise llfuse.FUSEError(errno.ENOENT)
  def flush(self,fh):
    print("flush",fh)
  def forget(self,inode_list):
    print("forget",inode_list)
  def getxattr(self,inode, name):
    if inode in self.inodemap:
      return self.inodemap[inode].getxattr(name)
    raise llfuse.FUSEError(errno.ENOENT)
  def setxattr(self,inode, name, value):
    if inode in self.inodemap:
      return self.inodemap[inode].setxattr(name,value)
    raise llfuse.FUSEError(errno.ENOENT)
  def removexattr(self,inode, name):
    if inode in self.inodemap:
      return self.inodemap[inode].removexattr(name)
    raise llfuse.FUSEError(errno.ENOENT)
  def releasedir(self,fh):
    print("releasedir",fh)
  def lookup(self,parent_inode, name):
    if parent_inode in self.inodemap:
      result=self.inodemap[parent_inode].lookup(name)
      self.inodemap[result.inode]=result
      return result.getattr()
    raise llfuse.FUSEError(errno.ENOENT)
  def listxattr(self,inode):
    if inode in self.inodemap:
      return self.inodemap[inode].listxattr()
  def readlink(self, inode):
    if inode in self.inodemap:
      return self.inodemap[inode].readlink()
    raise llfuse.FUSEError(errno.ENOENT)
  def opendir(self, inode):
    if inode in self.inodemap:
      return self.inodemap[inode].opendir()
    raise llfuse.FUSEError(errno.ENOENT)
  def readdir(self, inode, off):
    if inode in self.inodemap:
      if off == 0:
        for result in self.inodemap[inode].readdir():
          yield (result.name,result.getattr(),result.inode)
    else:
      raise llfuse.FUSEError(errno.ENOENT) 
  def unlink(self, inode_p, name):
    if inode in self.inodemap:
      return self.inodemap[inode].unlink(name)
    raise llfuse.FUSEError(errno.ENOENT)
  def rmdir(self, inode_p, name):
    if inode in self.inodemap:
      return self.inodemap[inode].rmdir(name) 
    raise llfuse.FUSEError(errno.NOENT)
  def symlink(self, inode_p, name, target, ctx):
    if inode in self.inodemap:
      return self.inodemap[inode].symlink(name,target,ctx)
    raise llfuse.FUSEError(errno.NOENT)
  def rename(self, inode_p_old, name_old, inode_p_new, name_new):
    if inode in self.inodemap:
      return self.inodemap[inode].rename(name_old,inode_p_new,name_new)
    raise llfuse.FUSEError(errno.ENOENT)
  def link(self, inode, new_inode_p, new_name):
    if inode in self.inodemap:
      return self.inodemap[inode].link(new_inode_p,new_name)
    raise llfuse.FUSEError(errno.ENOENT)
  def setattr(self, inode, attr):
    if inode in self.inodemap:
      return self.inodemap[inode].setattr(attr)
    raise llfuse.FUSEError(errno.EPERM)
  def mknod(self, inode_p, name, mode, rdev, ctx):
    if inode in self.inodemap:
      return self.inodemap[inode].mknod(name,mode,rdev,ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def mkdir(self, inode_p, name, mode, ctx):
    if inode in self.inodemap:
      return self.inodemap[inode].mkdir(name,mode,ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def statfs(self):
    print("statfs")
    return llfuse.StatvfsData()
  def open(self, inode, flags):
    if inode in self.inodemap:
      return self.inodemap[inode].open(flags)
    raise llfuse.FUSEError(errno.EPERM)
  def access(self, inode, mode, ctx):
    if inode in self.inodemap:
      return self.inodemap[inode].access(mode,ctx)
    return True
  def create(self, inode_parent, name, mode, flags, ctx):
    if inode in self.inodemap:
      return self.inodemap[inode].create(name,mode,flags,ctx)
    raise llfuse.FUSEError(errno.EPERM)
  def read(self, fh, offset, length):
    raise llfuse.FUSEError(errno.EPERM)
  def write(self, fh, offset, buf):
    raise llfuse.FUSEError(errno.EPERM)
  def release(self, fh):
    print("release",fh)

def parse_args():
    '''Parse command line'''
    parser = ArgumentParser()
    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    return parser.parse_args()

if __name__ == '__main__':
  options = parse_args()
  operations = Operations()
  llfuse.init(operations, options.mountpoint,
                [  'fsname=pymattockfs', "nonempty" ])
  try:
    llfuse.main(single=True)
  except:
    llfuse.close(unmount=False)
    raise
  llfuse.close()


