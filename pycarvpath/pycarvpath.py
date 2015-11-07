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
  def __init__(self,a1=True):
    self._h= ohash_algo(digest_size=32)
    self.offset=0
    self.isdone=False
    self.result="INCOMPLETE-OPPORTUNISTIC_HASHING"
    if isinstance(a1,bool):
      self.initialsize=0
      self.issparse=a1
    else:
      self.initialsize=a1
      self.issparse=False  
  def sparse(self,length):
    _h.update(bytearray(length).decode())
  def written_chunk(self,data,offset):
    self.initialsize =0; #No more guessing when we are ready, things are getting written so they get messy now.
    if offset < self.offset or self.isdone:
      self._h=ohash_algo(digest_size=32) #restart, we are no longer done, things are getting written.
      self.offset=0
      self.isdone=false
      self.result="INCOMPLETE-OPPORTUNISTIC_HASHING"
    if (offset > self.offset):
      #There is a gap!
      if self.issparse:
        difference = offset - self.offset
        times = difference / 1024
        for i in range(0,times):
          self.sparse(1024)
          self.sparse(difference % 1024)
        if offset == self.offset:
          self._h.update(data)
          self.offset += len(data)  
  def read_chunk(self,data,offset):
    if (not self.isdone) and offset <= self.offset and offser+len(data) > self.offset:
      #Fragment overlaps our offset; find the part that we didn't process yet.
      start=self.offset - offset
      datasegment = data[start:]
      self._h.update(datasegment)
      self.offset += len(datasegment)
      if self.offset > 0 and self.offset == self.initialsize:
        self.done()
  def done(self):
    if not isdone:
      self.result=self._h.hexdigest()

#A fragent represents a contiguous section of a higher level data entity.
class Fragment:
  #Constructor can either be called with a fragment carvpath token string or with an offset and size.
  #A fragment carvpath token string is formatted like '<offset>+<size>', for example : '2048+512'
  def __init__(self,a1,a2=None):
    if isinstance(a1,str):
      (self.offset,self.size) = map(int,a1.split('+'))
    else:
      self.offset=a1
      self.size=a2
  #Casting Fragment to a carvpath string
  def __str__(self):
    return str(self.offset) + "+" + str(self.size)
  #Comparing two fragments or a fragment and a sparse section. Please note that all fragments
  #are considered larger than any sparse section, regardless of sizes.
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
  def getoffset(self):
    return self.offset
  #This is how we distinguis a fragment from a sparse description
  def issparse(self):
    #A zero size fragment will act as a sparse description!
    return self.size == 0
  #If needed we can grow a fragment; only do this with the last fragment in an entity
  def grow(self,sz):
    self.size+=sz

#A Sparse object represents a higher level sparse definition that can be thought of as
#empty space that has no immage on any lower level data.
class Sparse:
  #Constructor can either be called with a sparse carvpath token string or with a size.
  #A sparse carvpath token string has the form 'S<size>', for example: 'S8192'
  def __init__(self,a1):
    if isinstance(a1,str):
      self.size = int(a1[1:])
    else:
      self.size = a1
  #Casting to a carvpath string
  def __str__(self):
    return "S" + str(self.size)
  #Comparing two sparse entities or a sparse entity and a fragment.
  #Please note that all fragments are considered larger than any sparse 
  #section, regardless of sizes.
  def __cmp__(self,other):
    if other.issparse() == False:
      return -1
    if self.size < other.size:
      return -1
    if self.size > other.size:
      return 1
    return 0
  #Calling this method on a Sparse will throw an runtime exception.
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

#An entity is an ordered  collection of Fragment and/or Sparse objects. Entities are the core concept within pycarvpath.
class _Entity:
  #An Entity constructor takes either a carvpath, a list of pre-made fragments or no constructor argument at all
  #if you wish to create a new empty Entity. You should probably not be instantiating your own _Entity objects.
  #Instead, create entities using the 'parse' method of a Contect object.
  #An _Entity carvpath consists of one or more Fragment and/or Sparse carvpath tokens seperated by a '_' character.
  #for example: '0+4096_S8192_4096+4096'
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
  #If desired, invoke the _Entity as a function to get the fragments it is composed of.
  def __cal__(self):
    for index in range(0,len(self.fragments)):
      yield self.fragments[index]
  #You may use square brackets to acces specific fragments.
  def __getitem__(self,index):
    return self.fragments[index]
  #Grow the entity by extending on its final fragment or, if there are non, by creating a first fragment with offset zero.
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
    if isinstance(other,_Entity):
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
    if isinstance(other,_Entity):
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
  #Appending two entities together, merging the tails if possible.
  def __add__(self,other):
    #Express a+b in terms of operator+= 
    rval=_Entity(self.longpathmap,self.maxfstoken)
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
    subentity=_Entity(self.longpathmap,self.maxfstoken)
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
    newfragment=_Entity(self.longpathmap,self.maxfstoken)
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
      rval.append(_Entity(ent1.longpathmap,ent1.maxfstoken))
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

class _Box:
  def __init__(self,lpmap,maxfstoken,fadvice,top):
    self.longpathmap=lpmap
    self.maxfstoken=maxfstoken
    self.fadvice=fadvice
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
  #Add a new entity to the box. Returns two entities: 
  # 1) An entity with all fragments that went from zero to one reference count.(can be used for fadvice purposes)
  # 2) An entity with all fragments already in the box before add was invoked (can be used for opportunistic hashing purposes).
  def add_batch(self,carvpath):
    if carvpath in self.entityrefcount:
      self.entityrefcount[carvpath] += 1
      ent=self.content[carvpath]
      return [_Entity(self.longpathmap,self.maxfstoken),ent]
    else:
      ent=_Entity(self.longpathmap,self.maxfstoken,carvpath)
      ent._stripsparse()
      ent=self.top.topentity.subentity(ent)
      self.content[carvpath]=ent
      self.entityrefcount[carvpath] = 1
      r= self._stackextend(0,ent)
      merged=r[0]
      for fragment in merged:
        self.fadvice(fragment.offset,fragment.size,True)
      return
  #Remove an existing entity from the box. Returns two entities:
  # 1) An entity with all fragments that went from one to zero refcount (can be used for fadvice purposes).
  # 2) An entity with all fragments still remaining in the box. 
  def remove_batch(self,carvpath):
    if not carvpath in self.entityrefcount:
      raise IndexError("Carvpath "+carvpath+" not found in box.")    
    self.entityrefcount[carvpath] -= 1
    if self.entityrefcount[carvpath] == 0:
      ent=self.content.pop(carvpath)
      del self.entityrefcount[carvpath]
      r= self._stackdiminish(len(self.fragmentrefstack)-1,ent)
      unmerged=r[0]
      for fragment in unmerged:
        self.fadvice(fragment.offset,fragment.size,False) 
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
  def lowlevel_writen_data(self,offset,data):
    #FIXME
    pass
  def lowlevel_read_data(self,offset,data):
    #FIXME
    pass
  def batch_hashing_done(self,carvpath):
    #FIXME
    pass
  def batch_hashing_value(self,carvpath):
    #FIXME
    pass

class _Top:
  def __init__(self,lpmap,maxfstoken,size=0):
    self.size=size
    self.topentity=_Entity(lpmap,maxfstoken,[Fragment(0,size)])
  def entity():
    return self.topentity
  def make_box(fadvice):
    return _Box(self.topentity.longpathmap,self.topentity.maxfstoken,fadvice,self.topentity)
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
      level=_Entity(self.longpathmap,self.maxfstoken,level)
      if not levelmin == None:
        level = levelmin.subentity(level)
      levelmin = level
    return level
  def make_top(self,size=0):
    return _Top(self.longpathmap,self.topentity,size)

def _test_fadvice_print(offset,size,advice):
  print("Here fadvice should be called on chunk (offset="+str(offset)+",size="+str(size)+" : "+str(advice))

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
    top=_Top(self.context.longpathmap,self.context.maxfstoken,topsize)
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
    top=_Top(self.context.longpathmap,self.context.maxfstoken,1000000)
    box=_Box(self.context.longpathmap,self.context.maxfstoken,_test_fadvice_print,top)
    box.add_batch("0+20000_40000+20000") #
    box.add_batch("10000+40000")         #
    box.add_batch("15000+30000")         #
    box.add_batch("16000+28000")
    box.add_batch("0+100000")            #
    box.add_batch("0+500000")
    box.add_batch("1000+998000")         #
    cp=box.priority_customsorted("R",reverse=True)
    print("Sorted R: "+str(cp))
    cp=box.priority_customsorted("r",reverse=True)
    print("Sorted r: "+str(cp))
    cp=box.priority_customsorted("O")
    print("Sorted O: "+str(cp))
    cp=box.priority_customsorted("S")
    print("Sorted S: "+str(cp))
    cp=box.priority_customsorted("D")
    print("Sorted D: "+str(cp))
    cp=box.priority_customsorted("W")
    print("Sorted W: "+str(cp))
    cp=box.priority_customsorted("ORS")
    print("Sorted ORS: "+str(cp))
    cp=box.priority_customsorted("DWS")
    print("Sorted DWS: "+str(cp))
    ol=box._overlaps(1550,600)
    print("Overlaps: "+str(ol))
    box.remove_batch("15000+30000")
    box.remove_batch("0+20000_40000+20000")
    box.remove_batch("1000+998000")
    box.remove_batch("0+100000")
    box.remove_batch("10000+40000")
    box.remove_batch("0+500000")
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
   
