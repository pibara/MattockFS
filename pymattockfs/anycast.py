#!/usr/bin/python
import random
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

class ModuleInstance:
  def __init__(self,modulename,instancehandle,module):
    self.module=module
    self.modulename=modulename
    self.instancehandle=instancehandle
    self.lastjobno=0
    self.currentjob=None
    self.currentjobhandle=None
    self.select_policy="S"
    self.sort_policy="HrdS"
    self.valid=True
  def teardown(self):
    if self.currentjob != None:
      self.currentjob.commit()
      self.currentjob = None
    if self.valid:
      self.module.unregister(self.instancehandle)
      self.valid=False
  def __del__(self):
    self.teardown()
  def unregister(self):
    self.teardown()
  def accept_job(self):
    if self.currentjob != None:
      self.currentjob.commit()
    if self.sort_policy=="K":
      self.currentjob=self.module.get_kickjob() 
    else:
      self.currentjob=self.module.anycast_pop(self.select_policy,self.sort_policy)
    if self.currentjob != None:
      return self.currentjob.jobhandle
    return None

class Job:
  def __init__(self,jobhandle,modulename,carvpath,router_state,mime_type,file_extension,allmodules,provenance=copy.deepcopy(list())):
    self.jobhandle=jobhandle
    self.modulename=modulename
    self.carvpath=carvpath
    self.router_state=router_state
    self.mime_type=mime_type
    self.file_extension=file_extension
    self.submit_info=[]
    self.allmodules=allmodules
    self.provenance = provenance
    self.mno = 1
    provenancerecord={}
    provenancerecord["job"]=jobhandle
    provenancerecord["module"]=modulename
    if len(provenance) == 0:
      provenancerecord["carvpath"]=carvpath
      provenancerecord["mime_type"]=mime_type
    self.provenance.append(provenancerecord)
    self.mutablehandle=None
  def __del__(self):
    if self.mutablehandle:
      carvpath=self.get_frozen_mutable()
      print "WARNING: Orphaned mutable: " + carvpath
  def commit(self):
    if self.jobhandle in self.allmodules.jobs:
      print "PROVENANCE:",self.provenance
      del self.allmodules.jobs[self.jobhandle] 
  def next_hop(self,module,state):
    self.allmodules[module].anycast_add(self.carvpath,state,self.mime_type,self.file_extension,self.provenance)
    del self.allmodules.jobs[self.jobhandle]
  def create_mutable(self,msize):
    mno=self.mno
    self.mno += 1
    self.mutablehandle = "M" + blake2b("M" + hex(mno)[2:].zfill(16),digest_size=32,key=self.jobhandle[:64]).hexdigest()
    self.allmodules.newdata[self.mutablehandle]=self.allmodules.rep.newmutable(msize)
  def get_mutable(self):
    return self.mutablehandle
  def get_frozen_mutable(self):
    if self.mutablehandle != None and self.mutablehandle in self.allmodules.newdata:
      carvpath=self.allmodules.newdata[self.mutablehandle]
      del self.allmodules.newdata[self.mutablehandle]
      self.mutablehandle=None
      return carvpath
    return None
  def submit_child(self,carvpath,nexthop,routerstate,mimetype,extension):
    provenance=list()
    provenancerecord={}
    provenancerecord["job"]=self.jobhandle
    provenancerecord["module"]=self.modulename
    provenancerecord["carvpath"]=carvpath
    provenancerecord["parent_carvpath"]=self.carvpath
    provenancerecord["mime_type"]=mimetype
    provenance.append(provenancerecord)  
    self.allmodules[nexthop].anycast_add(self.carvpath,state,self.mime_type,self.file_extension,self.provenance) 

class ModuleState:
  def __init__(self,modulename,strongname,allmodules,rep):
    self.name=modulename
    self.instances={}
    self.anycast={}
    self.lastinstanceno=0
    self.lastjobno=0
    self.secret=strongname
    self.weight=100              #rw extended attribute
    self.overflow=10             #rw extended attribute
    self.allmodules=allmodules
    self.rep=rep
  def register_instance(self):   #read-only (responsibility accepting) extended attribute.
    instanceno=self.lastinstanceno
    self.lastinstanceno += 1
    rval = "I" + blake2b("I" + hex(instanceno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.instances[rval]=ModuleInstance(self.name,rval,self)
    self.allmodules.instances[rval]=self.instances[rval]
    return rval
  def unregister(self,handle):
    if handle in self.instances:
      del self.instances[handle]
      del self.allmodules.instances[handle]
  def reset(self):             #writable extended attribute (setting to one results in reset)
    handles=self.instances.keys()
    for handle in handles:
      self.unregister(handle)
  def instance_count(self):   #read-only extended attribute
    return len(self.instances)
  def throttle_info(self):    #read-only extended attribute
    set_size=len(self.anycast)
    set_volume=self.rep.anycast_set_volume(self.anycast)
    return (set_size,set_volume)
  def anycast_add(self,carvpath,router_state,mime_type,file_extension):   #FIXME: UNTESTED !!!
    jobno=self.lastjobno
    self.lastjobno += 1
    jobhandle = "J" + blake2b("J" + hex(jobno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.anycast[jobhandle]=Job(jobhandle,self.name,carvpath,router_state,mime_type,file_extension,self.allmodules,[])
    self.allmodules.path_state[carvpath]="anycast"
    self.allmodules.path_module[carvpath]=self.name
  def get_kickjob(self):
    jobno=self.lastjobno
    self.lastjobno += 1
    jobhandle = "J" + blake2b("J" + hex(jobno)[2:].zfill(16),digest_size=32,key=self.secret[:64]).hexdigest()
    self.allmodules.jobs[jobhandle]=Job(jobhandle,"kickstart","S0","","application/x-zerosize","empty",self.allmodules,[])
    return self.allmodules.jobs[jobhandle]
  def anycast_pop(self,sort_policy,select_policy="S"): #FIXME: UNTESTED !!!
    if self.name != "loadbalance":
      best=self.rep.anycast_best(self.name,self.anycast,sort_policy)
      if best != None:
        self.allmodules[best]=self.anycast.pop(best)
        self.allmodules.path_state[self.allmodules[best].carvpath]="pending"
        self.allmodules.path_module[self.allmodules[best].carvpath]=self.name
        return self.allmodules[best]
    else:
      best=self.allmodules.selectmodule(select_policy)
      if best != None:
        best=anycast_pop(sort_policy,select_policy)
        self.allmodules.path_state[best.carvpath]="migrating"
        self.allmodules.path_module[best.carvpath]=self.name
        return best
      return None 

class ModulesState:
  def __init__(self,rep):
    self.rep=rep
    self.modules={}
    self.instances={}
    self.jobs={}
    self.newdata={}
    self.path_state={} 
    self.path_module={}
    random.seed()
    self.rumpelstiltskin=''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(0,64))
  def __getitem__(self,key):
    if not key in self.modules:
      strongname = "M" + blake2b("M" +key,digest_size=32,key=self.rumpelstiltskin).hexdigest()
      self.modules[key]=ModuleState(key,strongname,self,self.rep)
    return self.modules[key]
  def selectmodule(self,select_policy):
    moduleset=self.modules.keys()
    if len(moduleset) == 0:
      return None
    if len(moduleset) == 1:
      return moduleset[0]
    for letter in select_policy:
      moduleset=self.rep.anycast_best_modules(self,moduleset,letter) 
      if len(moduleset) == 1:
        return moduleset[0]
    return moduleset[0]
  def validmodulename(self,modulename):
    if len(modulename) < 2:
      return False
    if len(modulename) > 40:
      return False
    if not modulename.isalpha() and  modulename.islower():
      return False
    return True
  def validinstancecap(self,handle):
    if handle in self.instances:
      return True
    return False
  def validjobcap(self,handle):
    if handle in self.jobs:
      return True
    return False  
  def validnewdatacap(self,handle):
    if handle in self.newdata:
      return True
    return False

if __name__ == '__main__':
  print "We should probably write some test code here"
