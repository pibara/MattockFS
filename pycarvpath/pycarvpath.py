#!/usr/bin/python

class Fragment:
  def __init__(self,a1,a2=None):
    if isinstance(a1,basestring):
      (self.offset,self.size) = map(long,a1.split('+'))
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
    if isinstance(a1,basestring):
      self.size = long(a1[1:])
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
  global _hashfunction
  global _longpathtmap
  rval = _hashfunction(path)
  _longpathmap[rval] = path
  return "D" + rval

class Entity:
  def __init__(self,a1=None):
    global _longpathmap
    fragments=[]
    self.fragments=[]
    if isinstance(a1,basestring):
      if a1[0] == 'D':
        carvpath=_longpathmap[a1[1:]]
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
    if self.totalsize == other.totalsize and len(self.fragments) == len(other.fragments):
      for index in range(0,len(self.fragments)):
        if not self.fragments[index] == other.fragments[index]:
          return False
      return True
    else:
      return False
  def __add__(self,other):
    fragments=self.fragments
    if (self.fragments[-1].getoffset() + self.fragments[-1].getsize()) == other.getoffset():
      fragments = self.fragments[:-1]
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

def moduleinit(ltmap,hashfunct,maxfstokenlen):
  global _longpathmap
  global _hashfunction
  global _maxfstoken
  _longpathmap = ltmap
  _hashfunction = hashfunct
  _maxfstoken = maxfstokenlen

def parse(path):
  levelmin=None
  for level in path.split("/"):
    level=Entity(level)
    if levelmin != None:
      level = levelmin.subentity(level)
    levelmin = level
  return level 

class _Test:
  def testflatten(self,pin,pout):
    a=parse(pin)
    if str(a) != pout:
      print "FAIL: in='" + pin + "' expected='" + pout + "' result='" + str(a) + "'"
  def testrange(self,topsize,carvpath,expected):
    top=Top(topsize)
    entity=parse(carvpath)
    if top.test(entity) != expected:
      print "FAIL: topsize=",topsize,"path=",carvpath,"result=",(not expected)


if __name__ == "__main__":
  import blake2
  moduleinit({},lambda x: blake2.blake2b(x,hashSize=32),160)
  t=_Test()
  t.testflatten("0+20000_40000+20000/10000+20000","10000+10000_40000+10000")
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000","15000+5000_40000+5000")
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000","17500+2500_40000+2500")
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500","18750+1250_40000+1250")
  t.testflatten("0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500/625+1250","19375+625_40000+625")
  t.testflatten("0+20000_20000+20000/0+40000","0+40000")
  t.testflatten("0+20000_20000+20000","0+40000")
  t.testflatten("S100_S200","S300")
  t.testflatten("S1_S1","S2")
  t.testflatten("0+5","0+5")
  t.testflatten("0+0","S0")
  t.testflatten("20000+0","S0")
  t.testflatten("S0","S0")
  t.testflatten("20000+0_89765+0","S0")
  t.testflatten("1000+0_2000+0/0+0","S0")
  t.testflatten("0+0/0+0","S0")
  t.testflatten("0+100_101+100_202+100_303+100_404+100_505+100_606+100_707+100_808+100_909+100_1010+100_1111+100_1212+100_1313+100_1414+100_1515+100_1616+100_1717+100_1818+100_1919+100_2020+100_2121+100_2222+100_2323+100_2424+100","D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c65335251fe3fa480")
  t.testrange(200000000000,"0+100000000000/0+50000000",True)
  t.testrange(20000,"0+100000000000/0+50000000",False)
   
