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
import copy
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
  #Constructor can either be called with a fragment carvpath token string or with an offset and size
  def __init__(self,a1,a2=None):
    if isinstance(a1,str):
      (self.offset,self.size) = map(int,a1.split('+'))
    else:
      self.offset=a1
      self.size=a2
  #Casting to a carvpath string
  def __str__(self):
    return str(self.offset) + "+" + str(self.size)
  #Comparing two fragments or a fragment and a sparse entity
  def __cmp__(self,other):
    if other.issparse() == True:
      return 1
    if self.offset < other.offset:
      return -1
    if self.offset > other.offset:
      return 1
    if self.size < other.size:
      return -1
    if self.size > other.size:
      return 1
    return 0
  #we need a getter for this for ducktyping purposes
  def getoffset(self):
    return self.offset
  #This is how we distinguis a fragment from a sparse description
  def issparse(self):
    #A zero size fragment will act as a sparse description!
    return self.size == 0
  #If needed we can grow a fragment; only do this with the last fragment in an entity
  def grow(self,sz):
    self.size+=sz

class Sparse:
  #Constructor can either be called with a sparse carvpath token string or with a size.
  def __init__(self,a1):
    if isinstance(a1,str):
      self.size = int(a1[1:])
    else:
      self.size = a1
  #Casting to a carvpath string
  def __str__(self):
    return "S" + str(self.size)
  #Comparing two sparse entities or a sparse entity and a fragment
  def __cmp__(self,other):
    if other.issparse() == False:
      return -1
    if self.size < other.size:
      return -1
    if self.size > other.size:
      return 1
    return 0
  #we need a getter for this for ducktyping purposes and to get usefull exceptions on logic errors.
  def getoffset(self):
    raise RuntimeError("Sparse doesn't have an offset")
  #This is how we distinguis a fragment from a sparse description
  def issparse(self):
    return True
  #If needed we can grow a sparse region; only do this with the last fragment in an entity
  def grow(self,sz):
    self.size+=sz

#Helper function for creating either a sparse region or a fragment from a carvpath fragment/sparse token.
def _asfrag(fragstring):
  if fragstring[0] == 'S':
    return Sparse(fragstring)
  else:
    rval = Fragment(fragstring)
    if rval.size == 0:
      return Sparse("S0")
    return rval

class Entity:
  #An Entity constructor takes either a carvpath, a list of pre-made fragments or no constructor argument at all for a new empty Entity
  def __init__(self,lpmap,maxfstoken,a1=None):
    self.longpathmap=lpmap
    self.maxfstoken=maxfstoken
    fragments=[]
    self.fragments=[]
    if isinstance(a1,str):
      #Any carvpath starting with a capital D must be looked up in our longtoken database
      if a1[0] == 'D':
        carvpath=self.longpathmap[a1]
      else:
        carvpath=a1
      #Apply the _asfrag helper function to each fragment in the carvpath and use it to initialize our fragments list
      fragments=map(_asfrag,carvpath.split("_"))
    else:
      if isinstance(a1,list):
        #Take the list of fragments as handed in the constructor
        fragments = a1
      else:
        if a1 == None: 
          #We are creating an empty zero fragment entity here
          fragments=[]
        else:
          raise TypeError('Entity constructor needs a string or list of fragments'+str(a1))
    self.totalsize=0
    for frag in fragments:
      self.unaryplus(frag)
  def _asdigest(self,path):
    rval = "D" + blake2b(path,digest_size=32).hexdigest()
    self.longpathmap[rval] = path
    return rval
  def __cal__(self):
    for index in range(0,len(self.fragments)):
      yield self.fragments[index]
  def __getitem__(self,index):
    return self.fragments[index]
  def grow(chunksize):
    if len(self.fragments) == 0:
      self.fragments.append(Fragment(0,chunksize))
    else:
      self.fragments[-1].grow(chunksize)
    self.totalsize+=chunksize
  #Casting to a carvpath string
  def __str__(self):
    #Anything of zero size is represented as zero size sparse region.
    if len(self.fragments) == 0:
      return "S0"
    #Apply a cast to string on each of the fragment and concattenate the result using '_' as join character.
    rval = "_".join(map(str,self.fragments))
    #If needed, store long carvpath in database and replace the long carvpath with its digest.
    if len(rval) > self.maxfstoken:
      return self._asdigest(rval)
    else:
      return rval
  #Compare two entities in the default way
  def __cmp__(self,other):
    if isinstance(other,Entity):
      #First compare the fragments for all shared indexes untill one is unequal or we have processed them all.
      ownfragcount=len(self.fragments)
      otherfragcount=len(other.fragments)
      sharedfragcount=ownfragcount
      if otherfragcount < sharedfragcount:
        sharedfragcount=otherfragcount
      for index in range(0,sharedfragcount):
        if self.fragments[index] < other.fragments[index]:
          return -1
        if self.fragments[index] > other.fragments[index]:
          return 1
      #If all earlier fragments are equal, the entity with fragments remaining is he biggest.
      if ownfragcount < otherfragcount:
        return -1
      if ownfragcount > otherfragcount:
        return 1
      #All fragments are equal
      return 0
    else:
      #We define that entities are greater than all other types when compared.
      return 1
  #Python does not allow overloading of any operator+= ; this method pretends it does.
  def unaryplus(self,other):
    if isinstance(other,Entity):
      #We can either append a whole Entity
      for index in range(0,len(other.fragments)):
        self.unaryplus(other.fragments[index])
    else :
      #Or a single fragment.
      #If the new fragment is directly adjacent and of the same type, we don't add it but instead we grow the last existing fragment. 
      if len(self.fragments) > 0 and self.fragments[-1].issparse() == other.issparse() and (other.issparse() or (self.fragments[-1].getoffset() + self.fragments[-1].size) == other.getoffset()):
        self.fragments[-1]=copy.deepcopy(self.fragments[-1])
        self.fragments[-1].grow(other.size)
      else:
        #Otherwise we append the new fragment.
        self.fragments.append(other)
      #Meanwhile we adjust the totalsize member for the entity.
      self.totalsize += other.size
  def __add__(self,other):
    #Express a+b in terms of operator+= 
    rval=Entity(self.longpathmap,self.maxfstoken)
    rval.unaryplus(this)
    rval.unaryplus(other)
    return rval
  #Helper generator function for getting the per-fragment chunks for a subentity.
  #The function yields the parent chunks that fit within the offset/size indicated relative to the parent entity.
  def _subchunk(self,offset,size):
    #We can't find chunks beyond the parent total size
    if (offset+size) > self.totalsize:
      raise IndexError('Not within parent range')
    #We start of at offset 0 of the parent entity whth our initial offset and size. 
    start=0
    startoffset = offset
    startsize = size
    #Process each parent fragment
    for parentfrag in self.fragments:
      #Skip the fragments that fully exist before the ofset/size region we are looking for.
      if (start + parentfrag.size) > startoffset:
        #Determine the size of the chunk we need to process
        maxchunk = parentfrag.size + start - startoffset
        if maxchunk > startsize:
          chunksize=startsize
        else:
          chunksize=maxchunk
        #Yield the proper type of fragment
        if chunksize > 0:
          if parentfrag.issparse():
            yield Sparse(chunksize)
          else:
            yield Fragment(parentfrag.getoffset()+startoffset-start,chunksize)
          #Update startsize for the rest of our data
          startsize -= chunksize
        #Update the startoffset for the rest of our data
        if startsize > 0:
          startoffset += chunksize
        else:
          #Once the size is null, update the offset as to skip the rest of the loops
          startoffset=self.totalsize + 1
      start += parentfrag.size 
  #Get the projection of an entity as sub entity of an other entity.
  def subentity(self,childent):
    subentity=Entity(self.longpathmap,self.maxfstoken)
    for childfrag in childent.fragments:
      if childfrag.issparse():
        subentity.unaryplus(childfrag)
      else:
        for subfrag in self._subchunk(childfrag.offset,childfrag.size):
          subentity.unaryplus(subfrag)
    return subentity
  #Python has no operator=, so we use assigtoself
  def assigtoself(self,other):
    self.fragments=other.fragments
    self.totalsize=other.totalsize
  #Strip the entity of its sparse fragments and sort itsd non sparse fragments.
  #This is meant to be used for reference counting purposes inside of the Box.
  def _stripsparse(self):
    newfragment=Entity(self.longpathmap,self.maxfstoken)
    fragments=sorted(self.fragments)
    for i in range(len(fragments)):
      if fragments[i].issparse() == False:
        newfragment.unaryplus(fragments[i])
    self.assigtoself(newfragment)
  #Merge an other sorted/striped entity and return two entities: One with all fragments left unused and one with the fragments used
  #for merging into self.
  def merge(self,entity):
    rval=_merge_entities(self,entity)
    self.assigtoself(rval[0])
    return rval[1:]
  #Opposit of the merge function. 
  def unmerge(self,entity):
    rval=_unmerge_entities(self,entity)
    self.assigtoself(rval[0])
    return rval[1:]
  def overlaps(self,entity):
    test=lambda a,b: a and b
    return _fragapply(self,entity,test)
  def density(self,entity):
    t=lambda a,b: a and b
    r=_fragapply(self,entity,[t])
    rval= float(r[0].totalsize)/float(self.totalsize)
    return rval
  
#Helper functions for mapping merge and unmerge to the higher order _fragapply function.
def _merge_entities(ent1,ent2):
  selfbf = lambda a, b : a or b
  remfb = lambda a,b : a and b
  insfb = lambda a,b : (not a) and b
  return _fragapply(ent1,ent2,[selfbf,remfb,insfb])

def _unmerge_entities(ent1,ent2):
  selfbf = lambda a, b : a and (not b)
  remfb = lambda a,b : (not a) and b
  drpfb = lambda a,b : a and b
  return _fragapply(ent1,ent2,[selfbf,remfb,drpfb]) 

#Helper function for applying boolean lambda's to each ent1/ent2 overlapping or non-overlapping fragment
#and returning an Entity with all fragments that resolved to true for tha coresponding lambda.
def _fragapply(ent1,ent2,bflist):
    #If our third argument is a lambda, use it as test instead.
    if callable(bflist):
      test=bflist
      testmode=True
    else:
      test= lambda a,b: False
      testmode=False
    chunks=[]
    foreignfragcount = len(ent2.fragments)
    foreignfragindex = 0
    ownfragcount = len(ent1.fragments)
    ownfragindex=0
    masteroffset=0
    ownoffset=0
    ownsize=0
    ownend=0
    foreignoffset=0
    foreignsize=0
    foreignend=0
    discontinue = foreignfragcount == 0 and ownfragcount == 0
    #Walk through both entities at the same time untill we are done with all fragments and have no remaining size left.
    while not discontinue:
      #Get a new fragment from the first entity if needed and possible
      if ownsize == 0 and ownfragindex!= ownfragcount:
        ownoffset=ent1.fragments[ownfragindex].getoffset()
        ownsize=ent1.fragments[ownfragindex].size
        ownend=ownoffset+ownsize
        ownfragindex += 1
      #Get a new fragment from the second entity if needed and possible
      if foreignsize == 0 and foreignfragindex!= foreignfragcount:
        foreignoffset=ent2.fragments[foreignfragindex].getoffset()
        foreignsize=ent2.fragments[foreignfragindex].size
        foreignend=foreignoffset+foreignsize
        foreignfragindex += 1
      #Create an array of start and end offsets and sort them
      offsets=[]
      if ownsize >0:
        offsets.append(ownoffset)
        offsets.append(ownend)
      if foreignsize > 0:
        offsets.append(foreignoffset)
        offsets.append(foreignend)
      offsets=sorted(offsets)
      #Find the part we need to look at this time around the while loop
      firstoffset = offsets[0]
      secondoffset = offsets[1]
      if secondoffset == firstoffset:
        secondoffset = offsets[2]
      #Initialize boolens
      hasone=False
      hastwo=False
      #See if this chunk overlaps with either or both of the two entities.
      if ownsize >0 and ownoffset==firstoffset:
        hasone=True
      if foreignsize>0 and foreignoffset==firstoffset:
        hastwo= True
      fragsize = secondoffset - firstoffset
      #If needed, insert an extra entry indicating the false/false state.
      if firstoffset > masteroffset:
        if testmode:
          if test(False,False):
            return True
        else:
          chunks.append([masteroffset,firstoffset-masteroffset,False,False])
        masteroffset=firstoffset
      if testmode:
        if test(hasone,hastwo):
          return True
      else:
        #Than append the info of our (non)overlapping fragment
        chunks.append([firstoffset,fragsize,hasone,hastwo])
      #Prepare for the next time around the loop
      masteroffset=secondoffset
      if hasone:
        ownoffset=masteroffset
        ownsize-=fragsize
      if hastwo:
        foreignoffset=masteroffset
        foreignsize-=fragsize
      #Break out of the loop as soon as everything is done.
      if foreignfragindex == foreignfragcount and ownfragcount == ownfragindex and ownsize == 0 and foreignsize==0:
        discontinue=True
    if testmode:
      return False
    #Create an array with Entity objects, one per lambda.
    rval=[]
    for index in range(0,len(bflist)):
      rval.append(Entity(ent1.longpathmap,ent1.maxfstoken))
    #Fill each entity with fragments depending on the appropriate lambda invocation result.
    for index in range(0,len(chunks)):
      off=chunks[index][0]
      size=chunks[index][1]
      oldown=chunks[index][2]
      oldforeign=chunks[index][3]
      for index2 in range(0,len(bflist)):
        if bflist[index2](oldown,oldforeign):
          rval[index2].unaryplus(Fragment(off,size))
    return rval
 
class Box:
  def __init__(self,lpmap,maxfstoken,fadvice,top):
    self.longpathmap=lpmap
    self.maxfstoken=maxfstoken
    self.fadvice=fadvice
    self.top=top #Top entity used for all entities in the box
    self.content=dict() #Dictionary with all entities in the box.
    self.entityrefcount=dict() #Entity refcount for handling multiple instances of the exact same entity.
    self.fragmentrefstack=[] #A stack of fragments with different refcounts for keeping reference counts on fragments.
    self.fragmentrefstack.append(Entity(self.longpathmap,self.maxfstoken)) #At least one empty entity on the stack
  def __str__(self):
    rval=""
    for index in range(0,len(self.fragmentrefstack)):
      rval += "   + L" +str(index) + " : " + str(self.fragmentrefstack[index]) + "\n"
    for carvpath in self.content:
      rval += "   * " +carvpath + " : " + str(self.entityrefcount[carvpath]) + "\n"
    return rval
  #Add a new entity to the box. Returns two entities: 
  # 1) An entity with all fragments that went from zero to one reference count.(can be used for fadvice purposes)
  # 2) An entity with all fragments already in the box before add was invoked (can be used for opportunistic hashing purposes).
  def add(self,carvpath):
    if carvpath in self.entityrefcount:
      self.entityrefcount[carvpath] += 1
      ent=self.content[carvpath]
      return [Entity(self.longpathmap,self.maxfstoken),ent]
    else:
      ent=Entity(self.longpathmap,self.maxfstoken,carvpath)
      ent._stripsparse()
      ent=self.top.topentity.subentity(ent)
      self.content[carvpath]=ent
      self.entityrefcount[carvpath] = 1
      return self._stackextend(0,ent)
  #Remove an existing entity from the box. Returns two entities:
  # 1) An entity with all fragments that went from one to zero refcount (can be used for fadvice purposes).
  # 2) An entity with all fragments still remaining in the box. 
  def remove(self,carvpath):
    if not carvpath in self.entityrefcount:
      raise IndexError("Carvpath "+carvpath+" not found in box.")    
    self.entityrefcount[carvpath] -= 1
    if self.entityrefcount[carvpath] == 0:
      ent=self.content.pop(carvpath)
      del self.entityrefcount[carvpath]
      return self._stackdiminish(len(self.fragmentrefstack)-1,ent)
  #Request a list of entities that overlap from the box that overlap (for opportunistic hashing purposes).
  def overlaps(self,offset,size):
    ent=Entity(self.longpathmap,self.maxfstoken)
    ent.unaryplus(Fragment(offset,size))
    rval=[]
    for carvpath in self.content:
      if self.content[carvpath].overlaps(ent):
        rval.append(carvpath)
    return rval
  #From the entities pick one according to a strategy string.
  def pickspecial(self,strategy):
    if len(self.fragmentrefstack) == 0:
      return None
    myset=None
    for letter in strategy:
      findlargest=letter.isupper()
      uletter=letter.upper()
      if uletter == "R":
        myset=self._filterhighestrefcount(findlargest,myset)
      else :
        if uletter == "O":
          myset=self._filterlowestoffset(findlargest,myset)
        else:
          if uletter == "D":
            myset=self._filterrefcountdensity(findlargest,myset)
          else:
            if uletter == "S":
              myset=self._filtersize(findlargest,myset)
            else:
              if uletter == "W":
                myset=self._filterweighedrefcount(findlargest,myset)
              else:
                raise RuntimeError("Invalid letter for pickspecial policy")
      if len(myset) == 1:
        break 
    if myset == None:
      return set()
    return myset
  #Comand handler for R/r values for pickspecial.
  #select/filter on highest (R) or lowest (r) refcount fragment containing entities
  def _filterhighestrefcount(self,findlargest,inset=None):
    if inset == None:
      inset=set()
      for e in self.content.keys():
        inset.add(e)
    stacksize=len(self.fragmentrefstack)
    rset=set()
    if findlargest:
      looklevel=stacksize-1
      for index in range(looklevel,0,-1):
        hrentity=self.fragmentrefstack[index]
        for carvpath in inset:
          if hrentity.overlaps(self.content[carvpath]):
            rset.add(carvpath)
        if len(rset)!=0 :
          return rset
    else:
      if stacksize == 1:
        hrentity=self.fragmentrefstack[0]
        for carvpath in inset:
          if hrentity.overlaps(self.content[carvpath]):
            rset.add(carvpath)
        if len(rset)!=0 :
          return rset
      for index in range(1,stacksize):
        f=lambda a,b: a and not b
        r=_fragapply(self.fragmentrefstack[index-1],self.fragmentrefstack[index],[f])
        hrentity=r[0]
        for carvpath in inset:
          if hrentity.overlaps(self.content[carvpath]):
            rset.add(carvpath)
        if len(rset)!=0 :
          return rset
    return set()
  #Comand handler for O/o
  #select/filter on lowest (o) or highest (O) offset of first fragment in entity.
  def _filterlowestoffset(self,findlargest,inset=None):
    if inset == None:
      inset=set()
      for e in self.content.keys():
        inset.add(e)
    best = set()
    bestoffset=None
    for carvpath in inset:
      offset=self.content[carvpath].fragments[0].offset
      if bestoffset == None or ((not findlargest) and offset < bestoffset) or (findlargest and offset > bestoffset):
        best=set()
        bestoffset=offset
      if  offset == bestoffset:
        best.add(carvpath)
    return best
  #Comand handler for D/d
  #select/filter on highest(D) or lowest (d) density of highest refcount fragments in entity.
  def _filterrefcountdensity(self,findlargest,inset=None):
    if inset == None:
      inset=set()
      for e in self.content.keys():
        inset.add(e)
    best = set()
    bestdensity=None
    looklevel=len(self.fragmentrefstack)-1
    for index in range(looklevel,0,-1):
      best = set()
      bestdensity=None
      hrentity=self.fragmentrefstack[index]
      for carvpath in inset:
        if hrentity.overlaps(self.content[carvpath]):
          density=self.content[carvpath].density(hrentity)
          if bestdensity == None or findlargest and density>bestdensity or (not findlargest) and density<bestdensity:
            best = set()
            best.add(carvpath)
            bestdensity=density
          else:
            if bestdensity== density:
              best.add(carvpath)
      if len(best) > 0:
        return best
    return set()
  #Comand handler for W/w
  #select/filter on highest(W) or lowest(w) weigheded reference counts for all fragments in an entity. 
  def _filterweighedrefcount(self,findlargest,inset=None):
    if inset == None:
      inset=set()
      for e in self.content.keys():
        inset.add(e)
    bestdensity=100000000
    if findlargest:
       bestdensity=0
    bestset=set()
    for carvpath in inset:
      accumdensity=0
      for index in range(0,len(self.fragmentrefstack)):
        accumdensity+=self.content[carvpath].density(self.fragmentrefstack[index])
      if bestset==None or (findlargest and accumdensity > bestdensity) or ((not findlargest) and accumdensity < bestdensity):
        bestdensity = accumdensity
        bestset=set()
        bestset.add(carvpath)
      else:
        if accumdensity == bestdensity:
          bestset.add(carvpath)
    return bestset
  #select/filter on largest (S) or smallest (s) entity size.
  def _filtersize(self,biggest=True,inset=None):
    if inset == None:
      inset=set()
      for e in self.content.keys():
        inset.add(e)
    best = set()
    bestsize=None
    for carvpath in inset:
      cpsize = self.content[carvpath].totalsize
      if bestsize == None or (biggest and cpsize > bestsize) or ((not biggest) and cpsize < bestsize):
        best=set()
        bestsize=cpsize
      if bestsize == cpsize:
        best.add(carvpath)
    return best
  def _stackextend(self,level,entity):
    if not (level < len(self.fragmentrefstack)):
      self.fragmentrefstack.append(Entity(self.longpathmap,self.maxfstoken))
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

class Top:
  def __init__(self,lpmap,maxfstoken,size=0):
    self.size=size
    self.topentity=Entity(lpmap,maxfstoken,[Fragment(0,size)])
  def grow(self,chunk):
    self.size +=chunk
    self.topentity.grow(chunk)
  def test(self,child):
    try:
      b=self.topentity.subentity(child)
    except IndexError:
      return False
    return True

class Context:
  def __init__(self,lpmap,maxtokenlen):
    self.longpathmap=lpmap
    self.maxfstoken=maxtokenlen
  def parse(self,path):
    levelmin=None
    for level in path.split("/"):
      level=Entity(self.longpathmap,self.maxfstoken,level)
      if not levelmin == None:
        level = levelmin.subentity(level)
      levelmin = level
    return level

class _Test:
  def __init__(self,lpmap,maxtokenlen):
    self.context=Context(lpmap,maxtokenlen)
  def testadd(self,pin1,pin2,pout):
    a=self.context.parse(pin1)
    b=self.context.parse(pin2)
    c=self.context.parse(pout)
    a.unaryplus(b)
    if (a!=c):
      print("FAIL: '" + pin1 + " + " + pin2 + " = " + str(a) +  "' expected='" + pout + "'")
    else:
     print("OK: '" + pin1 + " + " + pin2 + " = " + str(a) +  "'")
  def teststripsparse(self,pin,pout):
    a=self.context.parse(pin)
    b=self.context.parse(pout)
    a._stripsparse()
    if a != b:
      print("FAIL: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
    else:
      print("OK: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
  def testflatten2(self,pin,pout):
    a=self.context.parse(pin)
    b=self.context.parse(pout)
    if a != b:
      print("FAIL: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
    else:
      print("OK: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
  def testflatten(self,pin,pout):
    a=self.context.parse(pin)
    if str(a) != pout:
      print("FAIL: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
    else:
      print("OK: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'")
    self.testflatten2(pin,pout)
  def testrange(self,topsize,carvpath,expected):
    top=Top(self.context.longpathmap,self.context.maxfstoken,topsize)
    entity=self.context.parse(carvpath)
    if top.test(entity) != expected:
      print("FAIL: topsize="+str(topsize)+"path="+carvpath+"result="+str(not expected))
    else:
      print("OK: topsize="+str(topsize)+"path="+carvpath+"result="+str(expected))
  def testsize(self,pin,sz):
    a=self.context.parse(pin)
    if a.totalsize != sz:
      print("FAIL: in='" + pin + "' expected='" + str(sz) + "' result='" + str(a.totalsize) + "'")
    else:
      print("OK: in='" + pin + "' expected='" + str(sz) + "' result='" + str(a.totalsize) + "'")
  def testmerge(self,p1,p2,pout):
    a=self.context.parse(p1)
    a._stripsparse()
    b=self.context.parse(p2)
    b._stripsparse()
    c=self.context.parse(pout)
    d=a.merge(b)
    if a!=c:
      print("FAIL : "+str(a)+"  "+str(d[0]) + " ;  "+str(d[1]))
    else:
      print("OK : "+str(a))
  def testbox(self):
    fadvice = lambda o,s,b: False
    top=Top(self.context.longpathmap,self.context.maxfstoken,1000000)
    box=Box(self.context.longpathmap,self.context.maxfstoken,fadvice,top)
    box.add("0+20000_40000+20000") #
    box.add("10000+40000")         #
    box.add("15000+30000")         #
    box.add("16000+28000")
    box.add("0+100000")            #
    box.add("0+500000")
    box.add("1000+998000")         #
    cp=box.pickspecial("R")
    print("Special R: "+str(cp))
    cp=box.pickspecial("r")
    print("Special r: "+str(cp))
    cp=box.pickspecial("O")
    print("Special O: "+str(cp))
    cp=box.pickspecial("o")
    print("Special o: "+str(cp))
    cp=box.pickspecial("S")
    print("Special S: "+str(cp))
    cp=box.pickspecial("s")
    print("Special s: "+str(cp))
    cp=box.pickspecial("D")
    print("Special D: "+str(cp))
    cp=box.pickspecial("d")
    print("Special d: "+str(cp))
    cp=box.pickspecial("W")
    print("Special W: "+str(cp))
    cp=box.pickspecial("w")
    print("Special w: "+str(cp))
    cp=box.pickspecial("ORs")
    print("Special ORs: "+str(cp))
    cp=box.pickspecial("dWS")
    print("Special dWS: "+str(cp))
    ol=box.overlaps(1550,600)
    print("Overlaps: "+str(ol))
    box.remove("15000+30000")
    box.remove("0+20000_40000+20000")
    box.remove("1000+998000")
    box.remove("0+100000")
    box.remove("10000+40000")
    box.remove("0+500000")
    #Now only 16000+28000 should remain
    if len(box.fragmentrefstack) != 1:
      print("BOXTEST FAIL: not exactly one entities remaining")
      print(str(box))
      return
    if str(box.fragmentrefstack[0]) != "16000+28000":
      print("BOXTEST FAIL: not expexted value of only box entry")
      print(str(box))
      return
    print("OK (Boxtest)")

if __name__ == "__main__":
  t=_Test({},160)
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
  t.testflatten("S200000/1000+9000","S9000")

  t.testrange(200000000000,"0+100000000000/0+50000000",True)
  t.testrange(20000,"0+100000000000/0+50000000",False)
  t.testsize("20000+0_89765+0",0)
  t.testsize("0+20000_40000+20000/10000+20000/5000+10000",10000)
  t.testsize("D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c65335251fe3fa480/350+100",100)
  t.teststripsparse("0+1000_S2000_1000+2000","0+3000")
  t.teststripsparse("1000+2000_S2000_0+1000","0+3000")
  t.teststripsparse("0+1000_S2000_4000+2000","0+1000_4000+2000")
  t.teststripsparse("4000+2000_S2000_0+1000","0+1000_4000+2000")
  t.testadd("0+1000_S2000_1000+2000","3000+1000_6000+1000","0+1000_S2000_1000+3000_6000+1000")
  t.testadd("0+1000_S2000","S1000_3000+1000","0+1000_S3000_3000+1000")
  t.testmerge("0+1000_2000+1000","500+2000","0+3000")
  t.testmerge("2000+1000_5000+100","100+500_800+800_4000+200_6000+100_7000+100","100+500_800+800_2000+1000_4000+200_5000+100_6000+100_7000+100")
  t.testmerge("2000+1000_5000+1000","2500+500","2000+1000_5000+1000")
  t.testmerge("500+2000","0+1000_2000+1000","0+3000")
  t.testmerge("0+1000_2000+1000","500+1000","0+1500_2000+1000")
  t.testmerge("S0","0+1000_2000+1000","0+1000_2000+1000")
  t.testmerge("0+60000","15000+30000","0+60000")
  t.testbox()
   
