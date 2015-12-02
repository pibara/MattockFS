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

try:
    from pyblake2 import blake2b
    ohash_algo=blake2b
except ImportError:
    import sys
    print("")
    print("\033[93mERROR:\033[0m Pyblake2 module not installed. Please install blake2 python module. Run:")
    print("")
    print("    sudo pip install pyblake2")
    print("")
    sys.exit()
try:
    #When this was written, pyblake2 didn't implement blake2bp yet. Hopefully it does in the future so the
    #Python implementation can be close to as fast as, and compatible with, the C++ implementation.
    from pyblake2 import blake2bp
    ohash_algo=blake2bp
except:
    pass

class _Opportunistic_Hash:
  def __init__(self,size):
    self._h= ohash_algo(digest_size=32)
    self.offset=0
    self.isdone=False
    self.result="INCOMPLETE-OPPORTUNISTIC_HASHING"
    self.fullsize=size
  def sparse(self,length):
    _h.update(bytearray(length).decode())
  def written_chunk(self,data,offset):
    if offset < self.offset or self.isdone:
      self._h=ohash_algo(digest_size=32) #restart, we are no longer done, things are getting written.
      self.offset=0
      self.isdone=false
      self.result="INCOMPLETE-OPPORTUNISTIC_HASHING"
    if (offset > self.offset):
      #There is a gap!
      difference = offset - self.offset
      times = difference / 1024
      for i in range(0,times):
        self.sparse(1024)
        self.sparse(difference % 1024)
      if offset == self.offset:
        self._h.update(data)
        self.offset += len(data)  
  def read_chunk(self,data,offset):
    if (not self.isdone) and offset <= self.offset and offset+len(data) > self.offset:
      #Fragment overlaps our offset; find the part that we didn't process yet.
      start=self.offset - offset
      datasegment = data[start:]
      self._h.update(datasegment)
      self.offset += len(datasegment)
      if self.offset > 0 and self.offset == self.fullsize:
        self.done()
  def done(self):
    if not self.isdone:
      self.result=self._h.hexdigest()
      self.isdone=True

class _OH_Entity:
  def __init__(self,ent):
    self.ent=copy.deepcopy(ent)
    self.ohash=_Opportunistic_Hash(self.ent.totalsize)
    self.roi=ent._getroi(0)
    print self.roi
  def  written_parent_chunk(self,data,parentoffset):
    parentfragsize=len(data)
    print parentoffset,parentfragsize,self.roi
    #Quick range of interest check.
    if parentoffset < self.roi[1] and (parentoffset+parentfragsize) > self.roi[0]:
      childoffset=0
      working=False
      updated=False
      for fragment in self.ent.fragments:
        if (not fragment.issparse()) and parentoffset >= fragment.offset and parentoffset < fragment.offset + fragment.size:
          working=True
          realstart = fragment.offset 
          realend = fragment.offset+fragment.size
          if parentoffset > realstart:
            realstart = parentoffset
          if (parentoffset + parentfragsize) < realend:
            realend=parentoffset + parentfragsize
          if realend > realstart:
           relrealstart = realstart-parentoffset
           relrealend = realend-parentoffset
           self.ohash.written_chunk(data[relrealstart:relrealend],childoffset+realstart-fragment.offset)  
          updated=True
        else:
          if working and fragment.issparse():
            self.ohash.sparse(fragment.size)
          else:
            working=False
        childoffset+=fragment.size
      if updated:
        #Update our range of interest.
        self.roi=self.ent._getroi(self.ohash.offset)
  def  read_parent_chunk(self,data,parentoffset):
    parentfragsize=len(data)
    print parentoffset,parentfragsize,self.roi
    #Quick range of interest check.
    if (not self.ohash.isdone) and parentoffset < self.roi[1] and (parentoffset+parentfragsize) > self.roi[0]:
      childoffset=0
      working=False
      updated=False
      print "inrange"
      for fragment in self.ent.fragments:
        print "FRAG",fragment.offset,fragment.size
        if (not fragment.issparse()) and parentoffset >= fragment.offset and parentoffset < fragment.offset + fragment.size:
          print "frag ",fragment.offset,fragment.size
          working=True
          realstart = fragment.offset
          realend = fragment.offset+fragment.size
          if parentoffset > realstart:
            realstart = parentoffset
          if (parentoffset + parentfragsize) < realend:
            realend=parentoffset + parentfragsize
          if realend > realstart:
           relrealstart = realstart-parentoffset
           relrealend = realend-parentoffset
           self.ohash.read_chunk(data[relrealstart:relrealend],childoffset+realstart-fragment.offset)
          updated=True
        else:
          if working and fragment.issparse():
            self.ohash.sparse(fragment.size)
          else:
            working=False
        childoffset+=fragment.size
      if updated:
        #Update our range of interest.
        self.roi=self.ent._getroi(self.ohash.offset)
      if self.ohash.isdone:
        print "DEBUG: Completed hash for "+str(self.ent)+" : " + self.ohash.result
  def hashing_offset(self):
    return self.ohash.offset
  def hashing_result(self):
    return self.ohash.result
  def hashing_isdone(self):
    return self.ohash.isdone
      
class OpportunisticHashCollection:
  def __init__(self,carvpathcontext):
    self.context=carvpathcontext
    self.ohash=dict()
  def add_carvpath(self,carvpath):
    ent=self.context.parse(carvpath)
    self.ohash[carvpath]=_OH_Entity(ent)
  def remove_carvpath(self,carvpath):
    del self.ohash[carvpath]   
  def lowlevel_written_data(self,offset,data):
    for carvpath in self.ohash.keys():
      self.ohash[carvpath].written_parent_chunk(data,offset)
  def lowlevel_read_data(self,offset,data):
    for carvpath in self.ohash.keys():
      self.ohash[carvpath].read_parent_chunk(data,offset)
  def hashing_isdone(self,carvpath):
    return self.ohash[carvpath].hashing_isdone()
  def hashing_value(self,carvpath):
    return self.ohash[carvpath].hashing_result()
  def hashing_offset(self,carvpath):
    return self.ohash[carvpath].hashing_offset()

if __name__ == "__main__":
  import carvpath
  context=carvpath.Context({},160)
  ohc=OpportunisticHashCollection(context)
  ohc.add_carvpath("10+5")
  ohc.add_carvpath("13+5")
  print
  print "Bad stuff"
  ohc.lowlevel_written_data(0,b"Bad stuff")
  print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
  print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5") 
  print
  print "Good stuff part 1"
  ohc.lowlevel_read_data(9,b"Good")
  print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
  print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5")
  print
  print "Good stuff part 2"
  ohc.lowlevel_read_data(12,b"d stuff")
  print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
  print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5")
  print ohc.hashing_value("10+5"), ohc.hashing_value("13+5")
