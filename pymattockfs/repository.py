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
import copy
import os
import fcntl
import carvpath
import refcount_stack

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
    readent=self.entity.subentity(_Entity(self.entity.longpathmap,self.entity.maxfstoken,[carvpath.Fragment(offset,size)]),True)
    result=b''
    for chunk in readent:
      os.lseek(self.fd, chunk.offset, 0)
      datachunk = os.read(self.fd, chunk.size)
      result += datachunk
      self.stack.lowlevel_read_data(chunk.offset,datachunk)
    return result

class Repository:
  def __init__(self,context,reppath,col):
    self.context=context
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
  def openro(self,cp,entity):
    return _OpenFile(self.stack,cp,True,entity,self.fd)

if __name__ == "__main__":
  import carvpath
  import opportunistic_hash
  context=carvpath.Context({},160)
  col=opportunistic_hash.OpportunisticHashCollection(context)
  rep=Repository(context,"/var/mattock/archive/0.dd",col)
  entity=context.parse("1234+5678")
  f1=rep.openro("1234+5678",entity)
  print rep.volume() 
