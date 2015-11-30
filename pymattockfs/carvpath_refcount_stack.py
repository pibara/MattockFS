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

import pycarvpath

def _defaultlt(al1,al2):
  for index in range(0,len(al1)):
    if al1[index] < al2[index]:
      return True
    if al1[index] > al2[index]:
      return False
  return False

class _CustomSortable:
  def __init__(self,carvpath,ltfunction,arglist):
    self.carvpath=carvpath
    self.ltfunction=ltfunction
    self.arglist=[]
    for somemap in arglist:
      self.arglist.append(somemap[carvpath])
  def __lt__(self,other):
    return self.ltfunction(self.arglist,other.arglist)

class CarvpathRefcountStack:
  def __init__(self,lpmap,maxfstoken,fadvise,ohashcollection,top):
    self.longpathmap=lpmap
    self.maxfstoken=maxfstoken
    self.fadvise=fadvise
    self.ohashcollection=ohashcollection
    self.top=top #Top entity used for all entities in the box
    self.content=dict() #Dictionary with all entities in the box.
    self.entityrefcount=dict() #Entity refcount for handling multiple instances of the exact same entity.
    self.fragmentrefstack=[] #A stack of fragments with different refcounts for keeping reference counts on fragments.
    self.fragmentrefstack.append(_Entity(self.longpathmap,self.maxfstoken)) #At least one empty entity on the stack
  def __str__(self):
    rval=""
    for index in range(0,len(self.fragmentrefstack)):
      rval += "   + L" +str(index) + " : " + str(self.fragmentrefstack[index]) + "\n"
    for carvpath in self.content:
      rval += "   * " +carvpath + " : " + str(self.entityrefcount[carvpath]) + "\n"
    return rval
  def __hash__(self):
    return hash(str(self))
  #Add a new entity to the box. Returns two entities: 
  # 1) An entity with all fragments that went from zero to one reference count.(can be used for fadvise purposes)
  # 2) An entity with all fragments already in the box before add was invoked (can be used for opportunistic hashing purposes).
  def add_carvpath(self,carvpath):
    if carvpath in self.entityrefcount.keys():
      self.entityrefcount[carvpath] += 1
      ent=self.content[carvpath]
      return [_Entity(self.longpathmap,self.maxfstoken),ent]
    else:
      ent=_Entity(self.longpathmap,self.maxfstoken,carvpath)
      self.ohashcollection.add_carvpath(carvpath)
      ent.stripsparse()
      ent=self.top.topentity.subentity(ent)
      self.content[carvpath]=ent
      self.entityrefcount[carvpath] = 1
      r= self._stackextend(0,ent)
      merged=r[0]
      for fragment in merged:
        self.fadvise(fragment.offset,fragment.size,True)
      return
  #Remove an existing entity from the box. Returns two entities:
  # 1) An entity with all fragments that went from one to zero refcount (can be used for fadvise purposes).
  # 2) An entity with all fragments still remaining in the box. 
  def remove_carvpath(self,carvpath):
    if not carvpath in self.entityrefcount.keys():
      raise IndexError("Carvpath "+carvpath+" not found in box.")    
    self.entityrefcount[carvpath] -= 1
    if self.entityrefcount[carvpath] == 0:
      ent=self.content.pop(carvpath)
      del self.entityrefcount[carvpath]
      self.ohashcollection.remove_carvpath(carvpath)
      r= self._stackdiminish(len(self.fragmentrefstack)-1,ent)
      unmerged=r[0]
      for fragment in unmerged:
        self.fadvise(fragment.offset,fragment.size,False) 
      return
  #Request a list of entities that overlap from the box that overlap (for opportunistic hashing purposes).
  def _overlaps(self,offset,size):
    ent=_Entity(self.longpathmap,self.maxfstoken)
    ent.unaryplus(Fragment(offset,size))
    rval=[]
    for carvpath in self.content:
      if self.content[carvpath].overlaps(ent):
        rval.append(carvpath)
    return rval
  def priority_customsort(self,params,ltfunction=_defaultlt,intransit=None,reverse=False):
    Rmap={}
    rmap={}
    omap={}
    dmap={}
    smap={}
    wmap={}
    arglist=[]
    startset=intransit
    if startset == None:
      startset=set(self.content.keys()) 
    stacksize=len(self.fragmentrefstack)
    for letter in params:
      if letter == "R":
        looklevel=stacksize-1
        for index in range(looklevel,0,-1):
          hrentity=self.fragmentrefstack[index]
          for carvpath in startset:
            if hrentity.overlaps(self.content[carvpath]):
              Rmap[carvpath]=True 
            else:
              Rmap[carvpath]=False
          if len(Rmap)!=0 :
              break
        arglist.append(Rmap) 
      else:
        if letter == "r":
          if stacksize == 1:
            hrentity=self.fragmentrefstack[0]
            for carvpath in startset:
              if hrentity.overlaps(self.content[carvpath]):
                rmap[carvpath]=True
              else:
                rmap[carvpath]=False
            if len(rmap)!=0 :
              break
          else:
            for index in range(1,stacksize):
              f=lambda a,b: a and not b
              r=_fragapply(self.fragmentrefstack[index-1],self.fragmentrefstack[index],[f])
              hrentity=r[0]
              for carvpath in startset:
                if hrentity.overlaps(self.content[carvpath]):
                  rmap[carvpath]=True
                else:
                  rmap[carvpath]=False 
              if len(rmap)!=0 :
                break
          arglist.append(rmap)        
        else :
          if letter == "O":
            for carvpath in startset:
              offset=None
              for frag in self.content[carvpath].fragments:
                if offset==None or frag.issparse == False and frag.offset < offset:
                  offset=frag.offset
              omap[carvpath]=offset
            arglist.append(omap)
          else:
            if letter == "D":
              looklevel=stacksize-1
              for index in range(looklevel,0,-1):
                hrentity=self.fragmentrefstack[index]
                for carvpath in startset:
                  if hrentity.overlaps(self.content[carvpath]):
                    dmap[carvpath]=self.content[carvpath].density(hrentity)
              arglist.append(dmap)
            else:
              if letter == "S":
                for carvpath in startset:
                  smap[carvpath]=self.content[carvpath].totalsize
                arglist.append(smap)
              else:
                if letter == "W":
                  for carvpath in startset:
                    accumdensity=0
                    for index in range(0,len(self.fragmentrefstack)):
                       accumdensity+=self.content[carvpath].density(self.fragmentrefstack[index])
                    wmap[carvpath]=accumdensity
                  arglist.append(wmap)
                else:
                  raise RuntimeError("Invalid letter for pickspecial policy")
    sortable=[]
    for carvpath in startset:
      sortable.append(_CustomSortable(carvpath,ltfunction,arglist))
    sortable.sort(reverse=reverse)
    for wrapper in sortable:
      yield wrapper.carvpath
  def priority_customsorted(self,params,ltfunction=_defaultlt,intransit=None,reverse=False):
    customsrt=[]
    for entity in self.priority_customsort(params,ltfunction,intransit,reverse):
      customsrt.append(entity)
    return customsrt
  def priority_custompick(self,params,ltfunction=_defaultlt,intransit=None,reverse=False):
    for entity in self.priority_customsort(self,params,ltfunction,intransit,reverse):
      return entity #A bit nasty but safes needles itterations.
  def _stackextend(self,level,entity):
    if not (level < len(self.fragmentrefstack)):
      self.fragmentrefstack.append(_Entity(self.longpathmap,self.maxfstoken))
    ent=self.fragmentrefstack[level]
    res=ent.merge(entity)
    merged=res[1]
    unmerged=res[0]
    if (len(unmerged.fragments)!=0) :
      self._stackextend(level+1,unmerged)  
    return [merged,unmerged]
  def _stackdiminish(self,level,entity):
    ent=self.fragmentrefstack[level]
    res=ent.unmerge(entity)
    unmerged=res[1]
    remaining=res[0]
    if len(self.fragmentrefstack[level].fragments) == 0:
      self.fragmentrefstack.pop(level)
    if len(remaining.fragments) > 0:
      if level == 0:
        raise RuntimeError("Data remaining after _stackdiminish at level 0")    
      res2=self._stackdiminish(level-1,remaining)
      unmerged.merge(res2[1])
      return [res2[1],unmerged]
    else:
      if level == 0:
        return [unmerged,remaining]
      else:
        return [remaining,unmerged];


if __name__ == "__main__":
  print "Need to write some tests here"
   
