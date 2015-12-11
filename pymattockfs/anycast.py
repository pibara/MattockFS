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
#This file contains an in-memory implementation of the base anycast functionality.
#Note that this implementation uses sorting algoritms for picking the highest priority 
#jobs from anycast sets (we replaced priority queues with sortable sets).
#An eventual implementation should provide either a journal or a persistency measure and
#should optimize the picking algoritm so it doesn't need to sort large sets.
import random
import copy
import provenance_log

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

#In-file-system representation of a module instance.
class ModuleInstance:
  def __init__(self,modulename,instancehandle,module):
    self.module=module
    self.modulename=modulename
    self.instancehandle=instancehandle
    self.currentjob=None
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

class CapabilityGenerator:
  def __init__(self):
    self.sequence=0
    random.seed()
    self.genesiscap="C" +''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(0,64))
  def __call__(self,parentcap=None):
    if parentcap == None:
      parentcap=self.genesiscap
    curseq=self.sequence
    self.sequence += 1
    return "C" + blake2b("C" + hex(curseq)[2:].zfill(16),digest_size=32,key=parentcap[1:65]).hexdigest()
    

class Frozen:
  def __init__(self,stack,carvpath):
    self.stack=stack
    self.carvpath=carvpath
    self.stack.add_carvpath(self.carvpath)
  def __del__(self):
    self.stack.remove_carvpath(self.carvpath)

class Mutable:
  def __init__(self,allmodules,msize,secret):
    self.mutablehandle = allmodules.capgen(secret)
    self.carvpath=allmodules.rep.newmutable(msize)
    self.lookup=allmodules.newdata
    self.lookup[self.mutablehandle]=self.carvpath
    self.stack=allmodules.rep.stack
    self.stack.add_carvpath(self.carvpath)
  def __del__(self):
    self.stack.remove_carvpath(self.carvpath)
    del self.lookup[self.mutablehandle]
    

#The core of the anycast funcionality is the concept of a job.
class Job:
  def __init__(self,jobhandle,modulename,carvpath,router_state,mime_type,file_extension,allmodules,provenance=None):
    self.jobhandle=jobhandle
    self.modulename=modulename
    self.carvpath=carvpath
    self.router_state=router_state
    self.mime_type=mime_type
    self.file_extension=file_extension
    self.submit_info=[]
    self.allmodules=allmodules
    self.mutable=None
    self.frozen=None
    if provenance == None:
      self.provenance= provenance_log.ProvenanceLog(jobhandle,modulename,carvpath,mime_type,journal=self.allmodules.journal,provenance_log=allmodules.provenance_log)
    else:
      self.provenance = provenance
      self.provenance(jobhandle,modulename)
  def __del__(self):
    if self.mutable != None:
      carvpath=self.get_frozen_mutable()
      #FIXME, we need better logging.
      print "WARNING: Orphaned mutable: " + carvpath
  def commit(self):
    if self.jobhandle in self.allmodules.jobs:
      del self.allmodules.jobs[self.jobhandle] 
  def next_hop(self,module,state):
    self.allmodules[module].anycast_add(self.carvpath,state,self.mime_type,self.file_extension,self.provenance)
    del self.allmodules.jobs[self.jobhandle]
  def create_mutable(self,msize):
    self.mutable=Mutable(self.allmodules,msize,self.jobhandle[:64])
  def get_mutable(self):
    if self.mutable != None:
      return self.mutable.mutablehandle
    return None
  def get_frozen_mutable(self):
    if self.mutable != None:
      carvpath=self.mutable.carvpath
      self.frozen=Frozen(self.allmodules.rep.stack,carvpath)
      self.mutable=None
      return carvpath
    if self.frozen != None:
      return self.frozen.carvpath
    return None
  def submit_child(self,carvpath,nexthop,routerstate,mimetype,extension):
    carvpath=carvpath.split("data/")[-1].split(".")[0]
    provenance=provenance_log.ProvenanceLog(self.jobhandle,self.modulename,carvpath,mimetype,self.carvpath,journal=self.allmodules.journal,provenance_log=self.allmodules.provenance_log)
    self.allmodules[nexthop].anycast_add(self.carvpath,routerstate,self.mime_type,self.file_extension,provenance) 

#The state shared by all module instances of a specific type. Also used when no instances are pressent.
class ModuleState:
  def __init__(self,modulename,capgen,allmodules,rep):
    self.name=modulename
    self.instances={}
    self.anycast={}
    self.secret=capgen()
    self.capgen=capgen
    self.weight=100              #rw extended attribute
    self.overflow=10             #rw extended attribute
    self.allmodules=allmodules
    self.rep=rep
  def register_instance(self):   #read-only (responsibility accepting) extended attribute.
    rval=self.capgen(self.secret)
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
  def anycast_add(self,carvpath,router_state,mime_type,file_extension,provenance):
    jobhandle = self.capgen(self.secret)
    self.anycast[jobhandle]=Job(jobhandle,self.name,carvpath,router_state,mime_type,file_extension,self.allmodules,provenance)
  def get_kickjob(self):
    self.anycast_add("S0","","application/x-zerosize","empty",None)
    return self.anycast_pop("S")
  def anycast_pop(self,sort_policy,select_policy="S"):
    if self.name != "loadbalance":
      best=self.rep.anycast_best(self.name,self.anycast,sort_policy)
      if best != None and best in self.anycast:
        job = self.anycast.pop(best)
        self.allmodules.jobs[best]=job
        return job
    else:
      best=self.allmodules.selectmodule(select_policy)
      if best != None:
        best=anycast_pop(sort_policy,select_policy)
        return best
      return None 

#State shared between different modules and a central coordination point.
class ModulesState:
  def __init__(self,rep,journal,provenance):
    self.rep=rep
    self.modules={}
    self.instances={}
    self.jobs={}
    self.newdata={}
    self.capgen=CapabilityGenerator()
    self.journal=open(journal, "a",0)
    self.provenance_log=open(provenance, "a",0)
  def __getitem__(self,key):
    if not key in self.modules:
      self.modules[key]=ModuleState(key,self.capgen,self,self.rep)
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
