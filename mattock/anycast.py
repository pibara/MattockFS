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
import json 

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

#In-file-system representation of a worker.
class Worker:
  def __init__(self,actorname,workerhandle,actor):
    self.actor=actor
    self.actorname=actorname
    self.workerhandle=workerhandle
    self.currentjob=None
    self.select_policy="S"
    self.sort_policy="HrdS"
    self.valid=True
  def teardown(self):
    if self.currentjob != None:
      self.currentjob.commit()
      self.currentjob = None
    if self.valid:
      self.actor.unregister(self.workerhandle)
      self.valid=False
  def __del__(self):
    self.teardown()
  def unregister(self):
    self.teardown()
  def accept_job(self):
    if self.currentjob != None:
      self.currentjob.commit()
    if self.sort_policy=="K":
      self.currentjob=self.actor.get_kickjob() 
    else:
      self.currentjob=self.actor.anycast_pop(self.select_policy,self.sort_policy)
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
  def __init__(self,actors,msize,secret):
    self.mutablehandle = actors.capgen(secret)
    self.carvpath=actors.rep.newmutable(msize)
    self.lookup=actors.newdata
    self.lookup[self.mutablehandle]=self.carvpath
    self.stack=actors.rep.stack
    self.stack.add_carvpath(self.carvpath)
  def __del__(self):
    self.stack.remove_carvpath(self.carvpath)
    del self.lookup[self.mutablehandle]
    

#The core of the anycast funcionality is the concept of a job.
class Job:
  def __init__(self,jobhandle,actorname,carvpath,router_state,mime_type,file_extension,actors,provenance=None):
    self.jobhandle=jobhandle
    self.actorname=actorname
    self.carvpath=carvpath
    self.router_state=router_state
    self.mime_type=mime_type
    self.file_extension=file_extension
    self.submit_info=[]
    self.actors=actors
    self.actors.rep.stack.add_carvpath(self.carvpath)
    self.mutable=None
    self.frozen=None
    if provenance == None:
      self.provenance= provenance_log.ProvenanceLog(jobhandle,actorname,router_state,carvpath,mime_type,extension=file_extension,journal=self.actors.journal,provenance_log=actors.provenance_log)
    else:
      self.provenance = provenance
      self.provenance(jobhandle,actorname,router_state)
  def __del__(self):
    self.actors.rep.stack.remove_carvpath(self.carvpath)
    if self.mutable != None:
      carvpath=self.get_frozen_mutable()
      #FIXME, we need better logging.
      print "WARNING: Orphaned mutable: " + carvpath
  def commit(self):
    if self.jobhandle in self.actors.jobs:
      del self.actors.jobs[self.jobhandle] 
  def next_hop(self,actor,state):
    self.actors[actor].anycast_add(self.carvpath,state,self.mime_type,self.file_extension,self.provenance)
    del self.actors.jobs[self.jobhandle]
  def create_mutable(self,msize):
    self.mutable=Mutable(self.actors,msize,self.jobhandle[:64])
  def get_mutable(self):
    if self.mutable != None:
      return self.mutable.mutablehandle
    return None
  def get_frozen_mutable(self):
    if self.mutable != None:
      carvpath=self.mutable.carvpath
      self.frozen=Frozen(self.actors.rep.stack,carvpath)
      self.mutable=None
      return carvpath
    if self.frozen != None:
      return self.frozen.carvpath
    return None
  def submit_child(self,carvpath,nexthop,routerstate,mimetype,extension):
    carvpath=carvpath.split("carvpath/")[-1].split(".")[0]
    provenance=provenance_log.ProvenanceLog(self.jobhandle,self.actorname,self.router_state,carvpath,mimetype,parentcp=self.carvpath,extension=self.file_extension,journal=self.actors.journal,provenance_log=self.actors.provenance_log)
    self.actors[nexthop].anycast_add(carvpath,routerstate,self.mime_type,self.file_extension,provenance) 

#The state shared by all workers of a specific type. Also used when no workers are pressent.
class Actor:
  def __init__(self,actorname,capgen,actors,rep):
    self.name=actorname
    self.workers={}
    self.anycast={}
    self.secret=capgen()
    self.capgen=capgen
    self.weight=100              #rw extended attribute
    self.overflow=10             #rw extended attribute
    self.actors=actors
    self.rep=rep
  def register_worker(self):   #read-only (responsibility accepting) extended attribute.
    rval=self.capgen(self.secret)
    self.workers[rval]=Worker(self.name,rval,self)
    self.actors.workers[rval]=self.workers[rval]
    return rval
  def unregister(self,handle):
    if handle in self.workers:
      del self.workers[handle]
      del self.actors.workers[handle]
  def reset(self):             #writable extended attribute (setting to one results in reset)
    handles=self.workers.keys()
    for handle in handles:
      self.unregister(handle)
  def worker_count(self):   #read-only extended attribute
    return len(self.workers)
  def throttle_info(self):    #read-only extended attribute
    set_size=len(self.anycast)
    #if set_size == 0:
    #  return [0,0]
    set_volume=self.rep.anycast_set_volume(self.anycast)
    return (set_size,set_volume)
  def anycast_add(self,carvpath,router_state,mime_type,file_extension,provenance):
    jobhandle = self.capgen(self.secret)
    self.anycast[jobhandle]=Job(jobhandle,self.name,carvpath,router_state,mime_type,file_extension,self.actors,provenance)
  def get_kickjob(self):
    self.anycast_add("S0","","application/x-zerosize","empty",None)
    return self.anycast_pop("S")
  def anycast_pop(self,sort_policy,select_policy="S"):
    if self.name != "loadbalance":
      best=self.rep.anycast_best(self.name,self.anycast,sort_policy)
      if best != None and best in self.anycast:
        job = self.anycast.pop(best)
        self.actors.jobs[best]=job
        return job
    else:
      best=self.actors.selectactor(select_policy)
      if best != None:
        best=anycast_pop(sort_policy,select_policy)
        return best
      return None 

#State shared between different actors and a central coordination point.
class Actors:
  def __init__(self,rep,journal,provenance):
    self.rep=rep
    self.actors={}
    self.workers={}
    self.jobs={}
    self.newdata={}
    self.capgen=CapabilityGenerator()
    self.provenance_log=open(provenance, "a",0)
    #Restoring old state from journal; commented out for now, BROKEN!
    #journalinfo={}
    #try :
    #  f=open(journal,'r')
    #except:
    #  f=None
    #if f != None:
    #  print "Harvesting old state from journal"
    #  for line in f:
    #    line=line.rstrip().encode('ascii','ignore')
    #    dat=json.loads(line)
    #    dtype=dat["type"]
    #    dkey=dat["key"]
    #    if dtype == "NEW":
    #      journalinfo[dkey]=[]
    #    if dtype != "FNL":
    #      journalinfo[dkey].append(dat["provenance"])
    #    else:
    #      del journalinfo[dkey]
    #  f.close()
    #  print "Done harvesting"
    self.journal=open(journal,"a",0)
    #if len(journalinfo) > 0:
    #  print "Processing harvested journal state"
    #  for needrestore in journalinfo:
    #    provenance_log=journalinfo[needrestore]
    #    print "Restoring : ",provenance_log
    #    self.journal_restore(provenance_log)
    #  print "State restored"
  def __getitem__(self,key):
    if not key in self.actors:
      self.actors[key]=Actor(key,self.capgen,self,self.rep)
    return self.actors[key]
  def journal_restore(self,journal_records):
    actor=journal_records[-1]["actor"]
    print "Restoring job for actor ", actor
    if len(journal_records) == 1:
      pl0=journal_records[0]
      print "  * Restoring without a provenance log: ", pl0["carvpath"],pl0["router_state"],pl0["mime"],pl0["extension"]
      self[actor].anycast_add(pl0["carvpath"],pl0["router_state"],pl0["mime"],pl0["extension"],None) 
    else:
      pl0=journal_records[0]
      print "* Creating first record for provenance log object for job"
      newpl=provenance_log.ProvenanceLog(pl0["job"],pl0["actor"],pl0["router_state"],pl0["carvpath"],pl0["mime"],pl0["extension"],journal=self.journal,provenance_log=self.provenance_log,restore=True)
      for subseq in journal_records[1:-1]:
        print "* Adding one more record to provenance log for job"
        newpl(subseq["jobid"],subseq["actor"],subseq["router_state"],restore=True)
      pln=journal_records[-1]
      print "* Restoring job with provenance log:",pl0["carvpath"],pln["router_state"],pl0["mime"],pl0["extension"],newpl
      self[actor].anycast_add(pl0["carvpath"],pln["router_state"],pl0["mime"],pl0["extension"],newpl)
  def selectactor(self,select_policy):
    actorset=self.actors.keys()
    if len(actorset) == 0:
      return None
    if len(actorset) == 1:
      return actorset[0]
    for letter in select_policy:
      actorset=self.rep.anycast_best_actors(self,actorset,letter) 
      if len(actorset) == 1:
        return actorset[0]
    return actorset[0]
  def validactorname(self,actorname):
    if len(actorname) < 2:
      return False
    if len(actorname) > 40:
      return False
    if not actorname.isalpha() and  actorname.islower():
      return False
    return True
  def validworkercap(self,handle):
    if handle in self.workers:
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
