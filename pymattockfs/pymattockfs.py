#!/usr/bin/python3
import llfuse
import errno
import stat
#from llfuse import FUSEError
from argparse import ArgumentParser

class Operations(llfuse.Operations):
  def __init__(self):
        super(Operations, self).__init__()
        print("init")
        self.inodedict=dict()
        self.nextinode=7
  def getattr(self, inode):
    print("getattr",inode)
    entry = llfuse.EntryAttributes()
    entry.st_ino = inode
    entry.generation = 0
    entry.entry_timeout = 30
    entry.attr_timeout = 30
    if inode == 1:
      entry.st_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH| stat.S_IXOTH
      entry.st_nlink = 42
    else:
      if inode == 2:
        entry.st_mode = stat.S_IFLNK | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        entry.st_nlink = 1
      else:
        entry.st_mode = stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        entry.st_nlink = 4
    entry.st_uid = 0
    entry.st_gid = 0
    entry.st_rdev = 0
    entry.st_size = 4096
    entry.st_blksize = 512
    entry.st_blocks = 1
    entry.st_atime_ns = 0
    entry.st_mtime_ns = 0
    entry.st_ctime_ns = 0
    return entry;
  def flush(self,fh):
    print("flush",fh)
  def forget(self,inode_list):
    print("forget",inode_list)
  def getxattr(self,inode, name):
    print("getxattr",inode, name)
    return b''
  def setxattr(self,inode, name, value):
    print("setxattr",inode, name, value)
  def removexattr(self,inode, name):
    print("removexattr",inode, name)
  def releasedir(self,fh):
    print("releasedir",fh)
  def lookup(self,parent_inode, name):
    print("lookup",parent_inode, name)
    if parent_inode == 1:
      if name == b'data.all':
        return self.getattr(2)
      if name == b'data':
        return self.getattr(3)
      if name == b'module':
        return self.getattr(4)
      if name == b'instance': 
        return self.getattr(5)
      if name == b'job':
        return self.getattr(6)
    raise llfuse.FUSEError(errno.ENOENT)
  def listxattr(self,inode):
    print("listxattr",inode)
  def readlink(self, inode):
    print("readlink",inode)
    if inode == 2:
      return b'data/0+123456789.all'
    raise llfuse.FUSEError(errno.EPERM)
  def opendir(self, inode):
    print("opendir",inode)
    return inode
  def readdir(self, inode, off):
    print("readdir",inode,off)
    if inode == llfuse.ROOT_INODE:
       if off == 0:
         yield (b'.',self.getattr(1),1)
         yield (b'..',self.getattr(1),1)
         yield (b'data.all',self.getattr(2),2)
         yield (b'data',self.getattr(3),3)
         yield (b'module',self.getattr(4),4)
         yield (b'instance',self.getattr(5),5)
         yield (b'job',self.getattr(6),6)
    else:
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


