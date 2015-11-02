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
#This code constitutes a Python port of the CarvPath library. It is meant to be used
#by forensic tools and frameworks in the service of implementing zero-storage carving
#facilities or other processes where designation of of potentially fragmented and sparse 
#sub-entities is esential.
#
try:
    from pyblake2 import blake2b
except ImportError:
    import sys
    print("")
    print("\033[93mERROR:\033[0m Pyblake2 module not installed. Please install blake2 python module. Run:")
    print("")
    print("    sudo pip install pyblake2")
    print("")
    sys.exit()


class Fragment:
  def __init__(self,a1,a2=None):
    if isinstance(a1,str):
      (self.offset,self.size) = map(int,a1.split('+'))
    else:
      self.offset=a1
      self.size=a2
  def __str__(self):
    return str(self.offset) + "+" + str(self.size)
  def getoffset(self):
    return self.offset
  def getsize(self):
    return self.size
  def __eq__(self,other):
    return other.issparse() == False and self.offset == other.offset and self.size == other.size
  def issparse(self):
    return False

class Sparse:
  def __init__(self,a1):
    if isinstance(a1,str):
      self.size = int(a1[1:])
    else:
      self.size = a1
  def __str__(self):
    return "S" + str(self.size)
  def getoffset(self):
    return -1
  def getsize(self):
    return self.size
  def __eq__(self,other):
    return other.issparse() == True and self.size == other.size
  def issparse(self):
    return True

def asfrag(fragstring):
  if fragstring[0] == 'S':
    return Sparse(fragstring)
  else:
    return Fragment(fragstring)

def asdigest(path):
  global _longpathtmap
  rval = "D" + blake2b(path,digest_size=32).hexdigest()
  _longpathmap[rval] = path
  return rval

class Entity:
  def __init__(self,a1=None):
    global _longpathmap
    fragments=[]
    self.fragments=[]
    if isinstance(a1,str):
      if a1[0] == 'D':
        carvpath=_longpathmap[a1]
      else:
        carvpath=a1
      fragments=map(asfrag,carvpath.split("_"))
    else:
      if isinstance(a1,list):
        fragments = a1
      else:
        if a1 == None: 
          fragments=[]
        else:
          raise TypeError('Entity constructor needs a string or list of fragments')
    self.totalsize=0
    for frag in fragments:
      if frag.getsize() > 0:
        if len(self.fragments) > 0 and self.fragments[-1].issparse() == frag.issparse() and (frag.issparse() or self.fragments[-1].offset+self.fragments[-1].size == frag.offset):
          self.fragments[-1].size += frag.size
        else:
          self.fragments.append(frag) 
        self.totalsize += frag.getsize()
  def __str__(self):
    global _maxfstoken
    if len(self.fragments) == 0:
      return "S0"
    rval = "_".join(map(str,self.fragments))
    if len(rval) > _maxfstoken:
      return asdigest(rval)
    else:
      return rval
  def getsize(self):
    return self.totalsize
  def __eq__(self,other):
    if other != None and (self.totalsize == other.totalsize) and len(self.fragments) == len(other.fragments):
      for index in range(0,len(self.fragments)):
        if not self.fragments[index] == other.fragments[index]:
          return False
      return True
    else:
      return False
  def __add__(self,other):
    print other
    fragments=self.fragments
    if (len(self.fragments) > 0) and (self.fragments[-1].getoffset() + self.fragments[-1].getsize() == other.getoffset()):
      fragments = self.fragments[:-1]
      print(len(self.fragments))
      fragments.append(Fragment(self.fragments[-1].getoffset(),self.fragments[-1].getsize()+other.getsize()))
    else:
      fragments=self.fragments[:]
      fragments.append(other)
    return Entity(fragments)
  def subchunk(self,offset,size):
    if (offset+size) > self.totalsize:
      raise IndexError('Not within parent range')
    start=0
    startoffset = offset
    startsize = size
    for parentfrag in self.fragments:
      if (start + parentfrag.getsize()) > startoffset:
        maxchunk = parentfrag.getsize() + start - startoffset
        if maxchunk > startsize:
          chunksize=startsize
        else:
          chunksize=maxchunk
        if parentfrag.issparse():
          yield Sparse(chunksize)
        else:
          yield Fragment(parentfrag.getoffset()+startoffset-start,chunksize)
        startsize -= chunksize
        if startsize > 0:
          startoffset += chunksize
        else:
          startoffset=self.totalsize + 1
      start += parentfrag.getsize() 
  def subentity(self,childent):
    subfrags=[]
    for childfrag in childent.fragments:
      if childfrag.issparse():
        subfrags.append(childfrag)
      else:
        for subfrag in self.subchunk(childfrag.offset,childfrag.size):
          subfrags.append(subfrag)
    return Entity(subfrags) 
  def stripsortsparse(self):
    newfragments=Entity()
    sortedfragments=sorted(self.fragments)
    for i in range(len(sortedfragments)):
      if sortedfragments[i].issparse() == False:
        newfragments += sortedfragments[i]
    self.fragments=newfragments.fragments
  def mergefragment(self,fragment):
    moffset=fragment.getoffset()
    msize=fragment.getsize()
    unmerged=[]
    merged=[]
    for frag in self.fragments:
      if msize > 0 and moffset+msize > frag.getoffset() and frag.getoffset()+frag.getsize() > moffset:
        if moffset < frag.getoffset():
          merged.append(Fragment(moffset,frag.getoffset()-moffset))
          msize -= frag.getoffset()-moffset
          moffset=frag.getoffset()
        nextsize=0
        nextoffset=frag.getoffset() + frag.getsize()
        if moffset+msize > frag.getoffset() + frag.getsize():
          nextsize=moffset+msize-frag.getoffset()-frag.getsize()
          msize -= nextsize
        unmerged.append(Fragment(moffset,msize))
        msize=nextsize
        moffset=nextoffset
    if msize > 0:
      merged.append(Fragment(moffset,msize))
    return [unmerged,merged]
  def flatten(self):
    flattened=[self.fragments[0]]
    for index in range(1,len(self.fragments)):
      if (self.fragments[index].getoffset() == self.fragments[index-1].getoffset()+self.fragments[index-1].getsize()):
        flattened[-1].size += self.fragments[index-1].getsize()
      else:
        flattened.append(self.fragments[index])
    self.fragments=flattened
  def merge(self,entity):
    newfragments=[]
    merged=Entity()
    unmerged=Entity()
    for i in range(len(entity.fragments)):
      print(entity.fragments)
      if entity.fragments[i].issparse() == False:
        res=self.mergefragment(entity.fragments[i])
        unmerged.fragments.append(res[0])
        merged.fragments.append(res[1])
    longlist=self.fragments
    longlist.append(merged)
    self.fragments=sorted(longlist)
    self.flatten()
    return [unmerged,merged]

class Box:
  def __init__(self,top):
    self.top=top #Top entity used for all entities in the box
    self.content=dict() #Dictionary with all entities in the box.
    self.entityrefcount=dict() #Entity refcount for handling multiple instances of the exact same entity.
    self.fragmentrefstack=[] #A stack of fragments with different refcounts for keeping reference counts on fragments.
    self.fragmentrefstack.append(Entity()) #At least one empty entity on the stack
  #Add a new entity to the box. Returns two entities: 
  # 1) An entity with all fragments that went from zero to one reference count.(can be used for fadvice purposes)
  # 2) An entity with all fragments already in the box before add was invoked (can be used for opportunistic hashing purposes).
  def add(self,carvpath):
    if carvpath in self.entityrefcount:
      self.entityrefcount[carvpath] += 1
      ent=self.content[carvpath]
      return [Entity(),ent]
    else:
      ent=Entity(carvpath)
      ent.stripsortsparse()
      ent=self.top.topentity.subentity(ent)
      self.content[carvpath]=ent
      self.entityrefcount[carvpath] = 1
      return self._stackextend(0,ent)
  #Remove an existing entity from the box. Returns two entities:
  # 1) An entity with all fragments that went from one to zero refcount (can be used for fadvice purposes).
  # 2) An entity with all fragments still remaining in the box. 
  def remove(self,carvpath):
    if carvpath in self.entityrefcount:
      self.entityrefcount[carvpath] -= 1
      ent=self.content[carvpath]
      return [Entity(),ent]
    else:
      ent=self.content.pop(carvpath)
      return self._stackdiminish(len(self.fragmentrefstack),ent)
  #Request a list of entities that overlap from the box that overlap (for opportunistic hashing purposes).
  def overlaps(self,offset,size):
    rval=[]
    for carvpath in self.content:
      if self.content.overlaps(offset,size):
        rval.append(carvpath)
    return rval
  #From the entities pick one according to a strategy string.
  #  R: selecht/filter on highest refcount fragment containing entities
  #  O: select/filter on lowest offset of first fragment in entity.
  #  D: select/filter on highest density of highest refcount fragments in entity.
  #  S: select/filter on largest entity size.
  #  s: select/filter on smallest entity size. 
  def pickspecial(self,strategy):
    if len(self.fragmentrefstack) == 0:
      return None
    myset=None
    for letter in strategy:
      if letter == "R":
        myset=_filterhighestrefcount(myset)
      if letter == "O":
        myset=_filterlowestoffset(myset)
      if letter == "D":
        myset=_filterrefcountdensity(myset)
      if letter == "S":
        myset=_filtersize(true,myset)
      if letter == "s":
        myset=_filtersize(false,myset)
    for carvpath in myset:
      return carvpath
    return None
  def _filterhighestrefcount(self,inset=None):
    if inset == None:
      inset=self.content.keys()
    stacksize=len(self.fragmentrefstack)
    looklevel=stacksize-1
    for index in range(looklevel,0,-1):
      lnentities=_highrefcountfragmentities(index)
      intersection = inset.intersection(lnentities)
      if len(intersection) > 0:
        return intersection
    return set()
  def _filterlowestoffset(self,inset=None):
    if inset == None:
      inset=self.content.keys()
    best = set()
    bestoffset=None
    for carvpath in inset:
      offset=self.content[carvpath].getoffset()
      if bestoffset == None or offset < bestoffset:
        best=set()
        bestoffset=offset
      if  offset == bestoffset:
        best.add(carvpath)
    return best
  def _filterrefcountdensity(self,inset=None):
    if inset == None:
      inset=self.content.keys()
    best = set()
    bestdensity=None
    for index in range(looklevel,0,-1):
      lnentities=_highrefcountfragmentities(index)
      intersection = inset.intersection(lnentities)
      if len(intersection) > 0:
        highcountentity=frag=self.fragmentrefstack[index]
        best = set()
        bestdensity=None
        for carvpath in inset:
          density=self.content[carvpath].density(highcountentity)
          if bestdensity == None or density > bestdensity:
            best=set()
            bestdensity=density
          if bestdensity == density:
            best.add(carvpath)
        return best
    return set()
  def _filtersize(self,biggest=True,inset=None):
    if inset == None:
      inset=self.content.keys()
    best = set()
    bestsize=None
    for carvpath in inset:
      cpsize = self.content[carvpath].getsize()
      if bestsize == None or (biggest and cpsize > bestsize) or ((not biggest) and cpsize < bestsize):
        best=set()
        bestsize=cpsize
      if bestsize == cpsize:
        best.add(carvpath)
    return best
  def _highrefcountfragmentities(self,level=-1):
    withhighrefcount = set()
    for index in self.fragmentrefstack[level]:
      frag=self.fragmentrefstack[index]
      withhighrefcount  |= self.overlaps(frag.getoffset(),frag.getsize()) 
  def _stackextend(self,level,entity):
    if not (level < len(self.fragmentrefstack)):
      self.fragmentrefstack.append(Entity())
    ent=self.fragmentrefstack[level]
    res=ent.merge(entity)
    unmerged=res[0]
    merged=res[1]
    if (len(unmerged.fragments)!=0) :
      self._stackextend(level+1,unmerged)  
    return [merged,unmerged]
  def _stackdiminish(self,level,entity):
    res=self.fragmentrefstack[level].unmerge(entity)
    unmerged=res[0]
    remaining=res[1]
    if remaining:
      if level == 0:
        raise RuntimeError    
      res2=self.stackdiminish(level-1,remaining)
      unmerged.merge(res2[1])
      return [res2[1],unmerged]
    else:
      if level == 0:
        return [unmerged,remaining]
      else:
        return [remaining,unmerged];

class Top:
  def __init__(self,size=0):
    self.size=size
    self.topentity=Entity([Fragment(0,size)])
  def grow(self,chunk):
    self.size +=chunk
    self.topentity=Entity([Fragment(0,size)])
  def test(self,child):
    try:
      b=self.topentity.subentity(child)
    except IndexError:
      return False
    return True

def moduleinit(ltmap,maxfstokenlen):
  global _longpathmap
  global _maxfstoken
  _longpathmap = ltmap
  _maxfstoken = maxfstokenlen

def parse(path):
  levelmin=None
  for level in path.split("/"):
    level=Entity(level)
    if not levelmin == None:
      level = levelmin.subentity(level)
    levelmin = level
  return level 

class _Test:
  def testflatten(self,pin,pout):
    a=parse(pin)
    if str(a) != pout:
      print("FAIL: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
    else:
      print("OK: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
  def testrange(self,topsize,carvpath,expected):
    top=Top(topsize)
    entity=parse(carvpath)
    if top.test(entity) != expected:
      print("FAIL: topsize=",topsize,"path=",carvpath,"result=",(not expected))
    else:
      print("OK: topsize=",topsize,"path=",carvpath,"result=",expected)
  def testbox(self):
    top=Top(1000000)
    box=Box(top)
    print "Box test not yet implemented."
    box.add("0+20000_40000+20000")
    box.add("10000+40000")
    box.add("15000+30000")
    box.add("16000+28000")
    box.add("0+100000")
    box.add("0+500000")
    box.add("1000+998000")
    box.remove("15000+30000")    

if __name__ == "__main__":
  moduleinit({},160)
  t=_Test()
  t.testflatten("0+0","S0");
  t.testflatten("S0","S0");
  t.testflatten("0+0/0+0","S0");
  t.testflatten("20000+0","S0");
  t.testflatten("20000+0_89765+0","S0");
  t.testflatten("1000+0_2000+0/0+0","S0");
  t.testflatten("0+5","0+5");
  t.testflatten("S1_S1","S2");
  t.testflatten("S100_S200","S300");
  t.testflatten("0+20000_20000+20000","0+40000");
  t.testflatten("0+20000_20000+20000/0+40000","0+40000");
  t.testflatten("0+20000_20000+20000/0+30000","0+30000");
  t.testflatten("0+20000_20000+20000/10000+30000","10000+30000");
  t.testflatten("0+20000_40000+20000/10000+20000","10000+10000_40000+10000");
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000","15000+5000_40000+5000");
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000","17500+2500_40000+2500");
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500","18750+1250_40000+1250");
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500/625+1250","19375+625_40000+625");
  t.testflatten("0+100_101+100_202+100_303+100_404+100_505+100_606+100_707+100_808+100_909+100_1010+100_1111+100_1212+100_1313+100_1414+100_1515+100_1616+100_1717+100_1818+100_1919+100_2020+100_2121+100_2222+100_2323+100_2424+100","D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c65335251fe3fa480");
  t.testflatten("0+100_101+100_202+100_303+100_404+100_505+100_606+100_707+100_808+100_909+100_1010+100_1111+100_1212+100_1313+100_1414+100_1515+100_1616+100_1717+100_1818+100_1919+100_2020+100_2121+100_2222+100_2323+100_2424+100/1+2488","D0e2ded6b35aa15baabd679f7d8b0a7f0ad393948988b6b2f28db7c283240e3b6");
  t.testflatten("D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c65335251fe3fa480/1+2488","D0e2ded6b35aa15baabd679f7d8b0a7f0ad393948988b6b2f28db7c283240e3b6");
  t.testflatten("D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c65335251fe3fa480/350+100","353+50_404+50");

  t.testrange(200000000000,"0+100000000000/0+50000000",True)
  t.testrange(20000,"0+100000000000/0+50000000",False)
  #t.testbox()
   
