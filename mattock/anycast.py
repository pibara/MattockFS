#!/usr/bin/python
# Copyright (c) 2015, Rob J Meijer.
# Copyright (c) 2015, University College Dublin
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. All advertising materials mentioning features or use of this software
#    must display the following acknowledgement:
#    This product includes software developed by the <organization>.
# 4. Neither the name of the <organization> nor the
#    names of its contributors may be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
#
# This file contains an in-memory implementation of the base anycast
# functionality. Note that this implementation uses sorting algoritms for
# picking the highest priority jobs from anycast sets (we replaced priority
# queues with sortable sets). An eventual implementation should provide either
# a journal or a persistency measure and should optimize the picking algoritm
# so it doesn't need to sort large sets.
import random
import copy
import provenance_log
import json
import os
import shutil 

try:
    from pyblake2 import blake2b
except ImportError:  # pragma: no cover
    import sys
    print("")
    print("\033[93mERROR:\033[0m Pyblake2 module not installed.")
    print("Please install blake2 python module.")
    print("Run:")
    print("")
    print("    sudo pip install pyblake2")
    print("")
    sys.exit()


# In-file-system representation of a worker.
class Worker:
    def __init__(self, actorname, workerhandle, actor, user, command, pid):
        self.actor = actor  # Refence to the actor this worker is instance of.
        self.actorname = actorname  # Name of the actor that we are worker for
        self.workerhandle = workerhandle  # Unique handle for this worker
        self.user = user
        self.command = command
        self.pid = pid
        self.currentjob = None  # The job currently being processed by us.
        # When not set to "S", the module selection policy for load balancing.
        self.module_select_policy = "S"
        # The pollicy for selecting the first job from the anycast set.
        self.job_select_policy = "H"
        self.valid = True  # Make sure we don't try to do cleanup twice.
    def still_running(self):
        if os.path.exists( "/proc/" + str(self.pid)):
            return True
        return False
    # Cleanup all pending state for the worker.
    def teardown(self):
        # If there is still a job marked as active, we have no other option
        # than to commit it.
        if self.currentjob is not None:
            self.currentjob.commit(worker=self)
            self.currentjob = None
        # Unregister the module and make sure we don't do so twice.
        if self.valid:
            self.actor.unregister(handle=self.workerhandle)
            self.valid = False

    # RAIIish way to implicitly clean up state.
    def __del__(self):
        self.teardown()

    # Explicit state clean up.
    def unregister(self):
        self.teardown()

    # Accept the next job according to the current job selection pollicy.
    def accept_job(self):
        # If for any reason we forgat to explicitly forward or commit the
        # previous job, we have no other option than comitting it now.
        if self.currentjob is not None:
            self.currentjob.commit(worker=self)
        # The "K" job select policy doesn't actually select a job from the
        # anycast set but creates one out of thin air as way to kickstart
        # new data.
        if self.job_select_policy == "K":
            self.currentjob = self.actor.get_kickjob(worker=self)
        else:
            # For all other policies, pop a job from our Actor's anycast set.
            self.currentjob = self.actor.anycast_pop(
              module_select_policy=self.module_select_policy,
              job_select_policy=self.job_select_policy,
              worker=self)
        # Return the handle of our new current job.
        if self.currentjob is not None:
            self.currentjob.worker = self
            self.currentjob.provenance.accept(actor=self.actorname,command=self.command,user=self.user)
            return self.currentjob.jobhandle
        return None


# A one-per-process object for creating sparse capabilities to be used as
# handle for workers, jobs or mutables.
class CapabilityGenerator:
    def __init__(self):
        self.sequence = 0  # Initiate counter.
        random.seed()  # Initialize random generator
        self.genesiscap = "C" + ''.join(
          random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in
          range(0, 64))  # Generate genesis capability.

    def __call__(self, parentcap=None):
        # If no parent cap is specified, use the genesis capability.
        if parentcap is None:
            parentcap = self.genesiscap
        # Claim and update a unique sequence number to use in capability
        # generation.
        curseq = self.sequence
        self.sequence += 1
        # Use Blake2B to create a new capability from the sequence number
        # and the parent capability.
        return "C" + blake2b(
            "C" + hex(curseq)[2:].zfill(16),
            digest_size=32,
            key=parentcap[1:65]).hexdigest()


# Simple RAIIish class for keeping track of Frozen-mutable CarvPath within the
# reference_counting stack.
class Frozen:
    # Constructor acquires
    def __init__(self, stack, carvpath):
        self.stack = stack
        self.carvpath = carvpath
        self.stack.add_carvpath(carvpath=self.carvpath)

    # Destructor releases.
    def __del__(self):
        self.stack.remove_carvpath(carvpath=self.carvpath)


# Simple RAIIish class for keeping track of mutable entity in capability
# lookup table and reference counting stack.
class Mutable:
    def __init__(self, capgen, repository, newdata, msize, secret):
        # Generate a brand new sparse-cap for this mutable
        self.mutablehandle = capgen(parentcap=secret)
        # Allocate the new mutable and remember the carvpath.
        self.carvpath = repository.newmutable(chunksize=msize)
        # Keep a reference to the newdata lookup map.
        self.lookup = newdata
        # Add ourself to the newdata lookup map
        self.lookup[self.mutablehandle] = self.carvpath
        # Keep a reference to the reference counting stack
        self.stack = repository.stack
        # Add ourself to the reference counting stack.
        self.stack.add_carvpath(carvpath=self.carvpath)

    def __del__(self):
        # Remove ourself from the reference counting stack
        self.stack.remove_carvpath(carvpath=self.carvpath)
        # Remove ourself from the newdata lookup map.
        del self.lookup[self.mutablehandle]


# The core of the anycast funcionality is the concept of a job.
class Job:
    def __init__(self, jobhandle, actorname, carvpath, router_state,
                 mime_type, file_extension, actors, context, stack,
                 journal, prov_log, jobs, capgen, newdata, col, rep,
                 provenance=None,worker=None, mt=None):
        self.jobhandle = jobhandle
        self.actorname = actorname
        # Process the carvpath and flatten or replace with longpath digest if
        # needed.
        self.carvpath = str(context.parse(carvpath))
        self.router_state = router_state
        self.mime_type = mime_type
        self.file_extension = file_extension
        self.submit_info = []
        self.actors = actors
        self.stack = stack
        # Increase the reference count for this Job's carvpath
        self.stack.add_carvpath(self.carvpath)
        # During a job, the worker may allocate one mutable at a time.
        self.mutable = None
        # That one mutable can get frozen so it can be submitted as child
        # entity.
        self.frozen = None
        self.journal = journal
        self.jobs = jobs
        self.capgen = capgen
        self.newdata = newdata
        self.col = col
        self.rep = rep
        user = "mattockfs"
        command = ["mattockfs"]
        if worker != None:
            user = worker.user
            command = worker.command
        self.mt = mt
        # Create a brand new provenance structure if non was passed in the
        # constructor.
        if provenance is None:
            self.provenance = provenance_log.ProvenanceLog(
              jobid=self.jobhandle,
              actor=self.actorname,
              router_state=self.router_state,
              carvpath=self.carvpath,
              mimetype=self.mime_type,
              extension=self.file_extension,
              journal=self.journal,
              provenance_log=prov_log,
              user=user,
              command=command,
              mt=mt)
        else:
            # Otherwise append some data to the existing structure.
            self.provenance = provenance
            self.provenance(
              jobid=self.jobhandle,
              actor=self.actorname,
              router_state=self.router_state,
              user=user,
              command=command)

    def __del__(self):
        # Update the reference count stack on delete.
        self.stack.remove_carvpath(carvpath=self.carvpath)
        # If we still have an unsubmitted mutable, there is not much we can do
        # other than log the problem.
        if self.mutable is not None:
            carvpath = self.get_frozen_mutable()
            # FIXME, we need better logging.
            print "WARNING: Orphaned mutable: " + carvpath

    def commit(self,worker=None):
        # Remove from the jobs lookup map on commit.
        if self.jobhandle in self.jobs:
            del self.jobs[self.jobhandle]

    def next_hop(self, actor, state):
        # Before deleting from the jobs lookup map, add a new job for this
        # carvpath to the anycast set of the named actor.
        self.actors[actor].anycast_add(
          carvpath=self.carvpath,
          router_state=state,
          mime_type=self.mime_type,
          file_extension=self.file_extension,
          provenance=self.provenance)
        del self.jobs[self.jobhandle]

    def create_mutable(self, msize):
        # Allocate a new mutable entity of the given size and bind it to the
        # current job.
        self.mutable = Mutable(capgen=self.capgen,
                               repository=self.rep,
                               newdata=self.newdata,
                               msize=msize,
                               secret=self.jobhandle[:64])

    def get_mutable(self):
        # Retreive the currently bound mutable.
        if self.mutable is not None:
            return self.mutable.mutablehandle
        return None

    def get_frozen_mutable(self):
        # Freeze the currently bound mutable if any.
        if self.mutable is not None:
            # Retreive the hidden carvpath from the mutable.
            carvpath = self.mutable.carvpath
            # Notify opportunistic hashing for this carvpath that the entity
            # is being frozen.
            self.col.freeze(carvpath=carvpath)
            # Create a Frozen mutable
            self.frozen = Frozen(stack=self.stack, carvpath=carvpath)
            # And delete the mutable itself.
            self.mutable = None
        if self.frozen is not None:
            # Return the new carvpath of the frozen mutable.
            return self.frozen.carvpath
        return None

    def submit_child(self, carvpath, nexthop, routerstate, mimetype,
                     extension):
        # Get the actual CarvPath part of the carvpath in the file-system.
        carvpath = carvpath.split("carvpath/")[-1].split(".")[0]
        # Create a brand new provenance structure
        provenance = provenance_log.ProvenanceLog(
                        self.jobhandle,
                        self.actorname,
                        self.router_state,
                        carvpath,
                        mimetype,
                        parentcp=self.carvpath,
                        parentjob=self.provenance.log[0]["jobid"],
                        extension=extension,
                        journal=self.journal,
                        provenance_log=self.actors.provenance_log,
                        user=self.worker.user,
                        command=self.worker.command,
                        mt = self.mt)
        # Add new child job to the anycast set of the indicated nexthop actor.
        self.actors[nexthop].anycast_add(carvpath=carvpath,
                                         router_state=routerstate,
                                         mime_type=self.mime_type,
                                         file_extension=extension,
                                         provenance=provenance,
                                         worker=self.worker)
    def restorepoint(self):
        self.provenance.restorepoint()
        

# The state shared by all workers of a specific type. Also used when no
# workers are pressent.
class Actor:
    def __init__(self, actorname, capgen, actors, rep, workers, journal,
                 provenance_log, jobs, newdata, context, stack, col, mt=None):
        self.name = actorname
        self.allworkers = workers
        self.journal = journal
        self.provenance_log = provenance_log
        self.jobs = jobs
        self.newdata = newdata
        self.context = context
        self.stack = stack
        self.col = col
        self.workers = {}
        self.anycast = {}
        self.secret = capgen()  # Generate a top-level secret for this actor.
        self.capgen = capgen
        self.weight = 100              # rw extended attribute
        self.overflow = 10             # rw extended attribute
        self.actors = actors
        self.rep = rep
        self.mt = mt

    def restorepoint(self):
        for jobname in self.anycast:
            self.anycast[jobname].restorepoint()
    # Register an instance for this Actor
    def register_worker(self, user, command, pid):   # read-only extended attribute.
        rval = self.capgen(parentcap=self.secret)  # Generate worker sparsecap
        # Make a new worker and register in the workers map.
        self.workers[rval] = Worker(actorname=self.name, workerhandle=rval,
                                    actor=self, user=user, command=command,
                                    pid=pid)
        # Also make the new worker part of a all-actors level lookup map.
        self.allworkers[rval] = self.workers[rval]
        return rval  # Return the sparse-cap.

    # Unregister an instance.
    def unregister(self, handle):
        if handle in self.workers:
            # Remove from per-actor map
            del self.workers[handle]
            # Remove from all-actors map
            del self.allworkers[handle]

    # Reset an actor by unregistering all workers.
    def reset(self):
        # Get a list of registered workers.
        handles = self.workers.keys()
        # Unregister each of them.
        for handle in handles:
            if not self.workers[handle].still_running():
                self.unregister(handle)

    # Count the workers for this actor.
    def worker_count(self):   # read-only extended attribute
        return len(self.workers)

    # Get the per-actor throttle info.
    def throttle_info(self):    # read-only extended attribute
        set_size = len(self.anycast)
        # Fetch the total volume of all entries in the set by asking the
        # repository.
        set_volume = self.rep.anycast_set_volume(self.anycast)
        return (set_size, set_volume)

    # Create and add a job to the anycast set for this actor.
    def anycast_add(self, carvpath, router_state, mime_type, file_extension,
                    provenance,worker=None):
        jobhandle = self.capgen(self.secret)  # Create a new Job sparse-cap.
        # Create a new job and add to the anycast set map.
        self.anycast[jobhandle] = Job(jobhandle=jobhandle,
                                      actorname=self.name,
                                      carvpath=carvpath,
                                      router_state=router_state,
                                      mime_type=mime_type,
                                      file_extension=file_extension,
                                      actors=self.actors,
                                      provenance=provenance,
                                      context=self.context,
                                      stack=self.stack,
                                      journal=self.journal,
                                      prov_log=self.provenance_log,
                                      jobs=self.jobs,
                                      capgen=self.capgen,
                                      newdata=self.newdata,
                                      col=self.col,
                                      rep=self.rep,
                                      worker=worker,
                                      mt=self.mt)
        return
    # Get a job to do a kickstart with.
    def get_kickjob(self,worker=None):
        # Add job to own anycast set.
        self.anycast_add(carvpath="S0",
                         router_state="",
                         mime_type="application/x-zerosize",
                         file_extension="empty",
                         provenance=None,
                         worker=worker)
        # And pop it immediately.
        return self.anycast_pop("S",worker=worker)

    # Pop a job from the anycast set using a given job select policy.
    def anycast_pop(self, job_select_policy, module_select_policy="S",worker=None):
        if self.name != "loadbalance":
            # For normal workers, get a best job from the repository according
            # to the select policy.
            best = self.rep.anycast_best(anycast=self.anycast,
                                         sort_policy=job_select_policy)
            if best is not None and best in self.anycast:
                # Pop the job from our set.
                job = self.anycast.pop(best)
                # Place it in the accessible jobs map.
                self.jobs[best] = job
                # Return the Job
                job.worker = worker
                return job
        else:
            # For our special "loadbalance" worker, select a module first.
            bestmodule = self.actors.selectactor(module_select_policy)
            if bestmodule is not None:
                # Than call ourselves for the most suitable module.
                best = self.actors[bestmodule].anycast_pop(job_select_policy,worker=worker)
                return best
        return None

class JournalFile:
    def __init__(self,jfname):
        self.active_file_name = jfname
        self.previous_file_name = jfname + "-previous"
        self.active_file = open(self.active_file_name, "a", 0)  # Unbuffered journal log.
    def write(self,data):
        self.active_file.write(data)
    def newfile(self):
        if os.path.exists(self.previous_file_name):
            os.unlink(self.previous_file_name)
        self.active_file.close()
        shutil.move(self.active_file_name,self.previous_file_name)
        self.active_file = open(self.active_file_name, "a", 0) # Unbuffered journal log. 
    def close(self):
        self.active_file.close()

# State shared between different actors and a central coordination point.
class Actors:
    def __init__(self, rep, journal, provenance, context, stack, col, mt = None):
        self.rep = rep
        self.context = context
        self.stack = stack
        self.col = col
        self.mt = mt
        self.actors = {}
        self.workers = {}
        self.jobs = {}
        self.newdata = {}
        self.capgen = CapabilityGenerator()  # Create capability generator.
        # Unbuffered provenance log.
        self.provenance_log = open(provenance, "a", 0)
        # Restoring old state from journal;
        journalinfo = {}
        if os.path.exists(journal):
            with open(journal, 'r') as f:
                for line in f:
                    line = line.rstrip().encode('ascii', 'ignore')
                    dat = json.loads(line)
                    dtype = dat["type"]
                    if dtype == "NEW":
                        dkey = dat["key"]
                        journalinfo[dkey] = []
                        journalinfo[dkey].append(dat["provenance"])
                    else:
                        if  dtype == "UPD":
                            dkey = dat["key"] 
                            journalinfo[dkey].append(dat["provenance"])
                        else:
                            if dtype == "FNL":
                                dkey = dat["key"]
                                del journalinfo[dkey]
                            else:
                                if dtype == "RPENT":
                                    dkey = dat["key"]
                                    journalinfo[dkey] = dat["provenance"]
        self.journal = JournalFile(journal)
        self.ticks = 0
        if len(journalinfo) > 0:
            for needrestore in journalinfo:
                provenance_log = journalinfo[needrestore]
                self.journal_restore(provenance_log,needrestore)
    def restorepoint(self):
        self.journal.newfile()
        self.journal.write("{\"type\" : \"RESTOREPOINT\", \"jobcount\" : " + str(len(self.jobs)) + " }\n")
        for jobkey in self.jobs:
            self.jobs[jobkey].restorepoint()
        for actorname in self.actors:
            self.actors[actorname].restorepoint()
    def tick(self):
        self.ticks = self.ticks + 1
        if self.ticks == 4096:
            self.ticks=0
            self.restorepoint()
    def __getitem__(self, key):
        # Any actor is made to exist by creating a new Actor for that name
        # if needed.
        if key not in self.actors:
            self.actors[key] = Actor(actorname=key,
                                     capgen=self.capgen,
                                     actors=self,
                                     rep=self.rep,
                                     workers=self.workers,
                                     journal=self.journal,
                                     provenance_log=self.provenance_log,
                                     jobs=self.jobs,
                                     newdata=self.newdata,
                                     context=self.context,
                                     stack=self.stack,
                                     col=self.rep.col,
                                     mt = self.mt)
        # self.tick()
        # Return the new or already existing actor object.
        return self.actors[key]
    #FIXME: untested code, might be OK, need tests first.
    def journal_restore(self, journal_records,oldkey):
         restorebug = True
         if restorebug:
             print "SKIP RESTORE! RESTORE CODE DISABLED (BROKEN)"
         else:
             last_record = journal_records[-1]
             #Try to restore job to its last queue
             actor = last_record["actor"]
             #If the job was active at restart, we can't just put it back in the queue.
             was_active = last_record["active"]
             #The last router state
             router_state = last_record["router_state"]
             #If the job was active, relay it to a module that can handle orphaned jobs.
             if was_active == True :
                 #Add the actor name to the router state to let the special module know what module orphaned the job.
                 router_state = router_state + ":" + actor
                 actor = "orphaned"
             pl0=journal_records[0]
             #Rebuild the provenance log object.
             newpl = provenance_log.ProvenanceLog(jobid=pl0["jobid"],
                                                  actor=pl0["actor"],
                                                  router_state=pl0["router_state"],
                                                  carvpath=pl0["carvpath"],
                                                  mimetype=pl0["mime"],
                                                  extension=pl0["extension"],
                                                  journal=self.journal,
                                                  provenance_log=self.provenance_log,
                                                  user=pl0["user"],
                                                  command=pl0["command"],
                                                  restore=True,
                                                  mt = self.mt)
             for subseq in journal_records[1:-1]:
                 newpl(jobid=subseq["jobid"],
                       actor=subseq["actor"],
                       router_state=subseq["router_state"],
                       user = subseq["user"],
                       command = subseq["command"],
                       restore=True)
             #Recreate the old job on the anycast.
             self[actor].anycast_add(
                carvpath=pl0["carvpath"],
                router_state=router_state,
                mime_type=pl0["mime"],
                file_extension=pl0["extension"],
                provenance=newpl)

    def selectactor(self, actor_select_policy):
        actorset = self.actors.keys()
        if len(actorset) == 0:
            return None
        if len(actorset) == 1:
            return actorset[0]
        for letter in actor_select_policy:
            actorset = self.rep.anycast_best_actors(self, actorset, letter)
            if len(actorset) == 1:
                return actorset[0]
        if len(actorset) == 0:
            return None
        return actorset[0]

    # Valid actor names are 2 to 40 alphanumeric characters.
    def validactorname(self, actorname):
        if len(actorname) < 2:
            return False
        if len(actorname) > 40:
            return False
        if not (actorname[0].isalpha() and actorname.isalnum() and actorname.islower()):
            return False
        return True

    # A valid workercap is a sparsecap thet exists in the global workers map.
    def validworkercap(self, handle):
        if handle in self.workers:
            return True
        return False

    # A valid jobcap is a cap that exists in the jobs map.
    def validjobcap(self, handle):
        if handle in self.jobs:
            return True
        return False

    # A valid newdata cap is a cap that exists in the newdata map.
    def validnewdatacap(self, handle):
        if handle in self.newdata:
            return True
        return False

if __name__ == '__main__':  # pragma: no cover
    print "We should probably write some test code here"
