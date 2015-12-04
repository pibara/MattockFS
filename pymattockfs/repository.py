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
import errno
import copy
import os
import fcntl
import carvpath
import refcount_stack
import opportunistic_hash

try:
  from os import posix_fadvise,POSIX_FADV_DONTNEED,POSIX_FADV_NORMAL
except:
  try:  
    from fadvise import posix_fadvise,POSIX_FADV_DONTNEED,POSIX_FADV_NORMAL
  except:
    import sys
    print("")
    print("\033[93mERROR:\033[0m fadvise module not installed. Please install fadvise python module. Run:")
    print("")
    print("    sudo pip install fadvise")
    print("")
    sys.exit()

class _FadviseFunctor:
  def __init__(self,fd):
    self.fd=fd
  def __call__(self,offset,size,willneed):
    if willneed:
      posix_fadvise(self.fd,offset,size,POSIX_FADV_NORMAL)
    else:
      posix_fadvise(self.fd,offset,size,POSIX_FADV_DONTNEED)

class _RaiiFLock:
  def __init__(self,fd):
    self.fd=fd
    fcntl.flock(self.fd,fcntl.LOCK_EX)
  def __del__(self):
    fcntl.flock(self.fd,fcntl.LOCK_UN)

class _OpenFile:
  def __init__(self,stack,cp,is_ro,entity,fd):
    self.is_ro = is_ro
    self.refcount = 1
    self.cp=cp
    self.stack=stack
    self.stack.add_carvpath(cp)
    self.entity=entity
    self.fd=fd
  def __del__(self):
    self.stack.remove_carvpath(self.cp)
  def read(self,offset,size):
    readent=self.entity.subentity(carvpath._Entity(self.entity.longpathmap,self.entity.maxfstoken,[carvpath.Fragment(offset,size)]),True)
    result=b''
    for chunk in readent:
      #FIXME: Check to make this attomic
      os.lseek(self.fd, chunk.offset, 0)
      datachunk = os.read(self.fd, chunk.size)
      result += datachunk
      self.stack.ohashcollection.lowlevel_read_data(chunk.offset,datachunk)
    return result
  def write(self,offset,data):
    size=len(data)
    writeent=self.entity.subentity(carvpath._Entity(self.entity.longpathmap,self.entity.maxfstoken,[carvpath.Fragment(offset,size)]),True)
    dataindex=0
    for chunk in writeent:
      chunkdata=data[dataindex:dataindex+chunk.size]
      dataindex += chunk.size
      #FIXME: Check to make this attomic
      os.lseek(self.fd, chunk.offset, 0)
      os.write(self.fd,chunkdata)
      self.stack.ohashcollection.lowlevel_written_data(chunk.offset,chunkdata)
    return size

class Repository:
  def __init__(self,reppath,context):
    self.context=context
    col=opportunistic_hash.OpportunisticHashCollection(context)
    self.openfiles={}
    self.lastfd=1
    self.fd=os.open(reppath,(os.O_RDWR | os.O_LARGEFILE | os.O_NOATIME | os.O_CREAT))
    cursize=os.lseek(self.fd,0,os.SEEK_END) 
    posix_fadvise(self.fd,0,cursize,POSIX_FADV_DONTNEED)
    self.top=self.context.make_top(cursize)
    fadvise=_FadviseFunctor(self.fd)
    self.stack=refcount_stack.CarvpathRefcountStack(self.context,fadvise,col)
  def __del__(self):
    os.close(self.fd)
  def _grow(self,newsize):
    l=_RaiiFLock(self.fd)
    os.ftruncate(self.fd,newsize)
  def newmutable(self,chunksize):
    chunkoffset=self.top.size
    self._grow(chunkoffset+chunksize) 
    self.top.grow(chunksize)
    cp=str(carvpath._Entity(self.context.longpathmap,self.context.maxfstoken,[carvpath.Fragment(chunkoffset,chunksize)])) 
    self.stack.add_carvpath(cp)
    return cp
  def volume(self):
    if len(self.stack.fragmentrefstack) == 0:
      return 0
    return self.stack.fragmentrefstack[0].totalsize
  def full_archive_size(self):
    return self.top.size
  def validcarvpath(self,cp):
    try:
      ent=self.context.parse(cp)
      rval=self.top.test(ent)
      return rval
    except:
      return False
  def flatten(self,basecp,subcp):
    try:
      ent=self.context.parse(basecp + "/" + subcp)
      return str(ent)
    except:
      return None
  def anycast_set_volume(self,anycast):
    volume=0
    for jobid in anycast:
      volume += self.context.parse(anycast[jobid].carvpath).totalsize
    return volume
  def anycast_best(self,modulename,anycast,sort_policy):
    if len(anycast) > 0:
      cp2key={}
      for anycastkey in anycast.keys():
        cp=anycast[anycastkey].carvpath
        cp2key[cp]=anycastkey
      bestcp=self.stack.priority_custompick(sort_policy,intransit=cp2key.keys()) 
      return cp2key[bestcp]
    return None
  def getTopThrottleInfo(self):
    totalsize=self.top.size
    normal=self.volume()
    dontneed=totalsize-normal
    return (normal,dontneed)
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
  def open(self,carvpath,path,readonly=True):
    ent=self.context.parse(carvpath)
    if path in self.openfiles:
      self.openfiles[path].refcount += 1
    else :
      ent=self.context.parse(carvpath)
      self.openfiles[path]=_OpenFile(self.stack,carvpath,readonly,ent,self.fd)
    return 0
  def read(self,path,offset,size):
    if path in self.openfiles: 
      return self.openfiles[path].read(offset,size)
    return -errno.EIO
  def write(self,path,offset,data):
    print "READ"
    if path in self.openfiles:
      return self.openfiles[path].write(offset,data)
    print "IOE"
    return -errno.EIO
  def flush(self):
    return os.fsync(self.fd)
  def close(self,path):
    self.openfiles[path].refcount -= 1
    if self.openfiles[path].refcount < 1:
      del self.openfiles[path]
    return 0
    

if __name__ == "__main__":
  import carvpath
  import opportunistic_hash
  context=carvpath.Context({},160)
  col=opportunistic_hash.OpportunisticHashCollection(context)
  rep=Repository(context,"/var/mattock/archive/0.dd",col)
  entity=context.parse("1234+5678")
  f1=rep.openro("1234+5678",entity)
  print rep.volume() 
