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
# This file contains the Python API to be used by the future Mattock framework.
# It is a wrapper for the extended attribute interface offered by MattockFS.
#
import xattr
import os
import os.path
import re
import json
from time import sleep
import carvpath


# Object representing a file under $MP/carvpath/
class _CarvPathFile:
    def __init__(self, mp, cp, context):
        self.mp = mp
        dotpos = cp.rfind(".")
        if dotpos == -1:
            self.dir_carvpath = cp
            self.file_carvpath = cp + ".dat"
        else:
            self.dir_carvpath = cp.split(".")[0]
            self.file_carvpath = cp
        self.context = context
        filpath = mp + "/" + self.file_carvpath
        if not os.path.isfile(filpath):
            raise IndexError("Invalid CarvPath " + cp)
        self.xa = xattr.xattr(filpath)

    # Get the path of the pseudo file.
    def as_file_path(self):
        return self.mp + "/" + self.file_carvpath

    def as_dir_path(self):
        return self.mp + "/" + self.dir_carvpath

    # Get as CarvPath entity.
    def as_entity(self):
        return self.context.parse(self.dir_carvpath.split("/")[-1])

    # Using a valid nested carvpath, get a sub CarvPath as CarvPathFile object.
    def __getitem__(self, cp):
        # Compose a two level CarvPath.
        twolevel = self.mp + "/" + self.dir_carvpath + "/" + cp
        # Turn it into a single level CarvPath by asking MattockFS for the
        # symbolic link.
        symlink = (self.mp + "/" + self.dir_carvpath + "/" +
                   os.readlink(twolevel))
        # The symlink contains '..' at first, update to canonical.
        absolute_symlink = os.path.abspath(symlink)
        # Finaly remove the mountpoint part so we are relative to $MP again.
        cp2 = absolute_symlink[len(self.mp)+1:]
        # Return a new CarvPatgFile object/
        return _CarvPathFile(mp=self.mp, cp=cp2, context=self.context)

    # Retreiver opportunistic hash exteded attibute.
    def opportunistic_hash(self):
        st = self.xa["user.opportunistic_hash"].split(";")
        return {"hash": st[0],
                "hash_offset": int(st[1])}

    # Retreive fadvise info extended attribute data.
    def fadvise_status(self):
        st = self.xa["user.fadvise_status"].split(";")
        return {"normal": int(st[0]), "dontneed": int(st[1])}

    def force_fadvise(self,s):
        self.xa["user.force_fadvise"] = s

    # Retreive the size of the entity.
    def file_size(self):
        return self.as_entity().totalsize


# Object representing a private Job control file under $MP/job/<CAP>.ctl
class _Job:
    def __init__(self, mp, job_ctl, context):
        self.isdone = False  # Keep from doing same cleanup twice.
        self.mp = mp
        self.ctl = xattr.xattr(job_ctl)
        # Proactively fetch routing state from previous actor.
        rstate = self.ctl["user.routing_info"].split(";")
        self.router_state = rstate[1]
        self.jobactor = rstate[0]
        # Proactively retreive job carvpath and create a CarvPathFile object.
        self.carvpath = _CarvPathFile(
                           mp=mp,
                           cp=self.ctl["user.job_carvpath"],
                           context=context)
        self.newdata = None  # There is no active newdata yet for this job.

    # Allocate a repository chunk for storage of derived job child (meta) data
    def childdata(self, datasize):
        if self.isdone is False:
            # Two step process, first allocate the data.
            self.ctl["user.allocate_mutable"] = str(datasize)
            # Than retreive a link to the new mutable data chunk.
            self.newdata = self.mp + "/" + self.ctl["user.current_mutable"]
            return self.newdata
        return None

    # Freeze the current child data object and retreive reference to immutable.
    def frozen_childdata(self):
        if self.isdone is False:
            try:
                return self.ctl["user.frozen_mutable"]
            except:
                return None
        return None

    # Submit a CarvPath as being a child of the current job.
    # This can be a CarvPath that defines a sub-carvpath of the job carvpath
    # or it can be the carvpath of a frozen child (meta)data carvpath.
    def childsubmit(self, carvpath, nextactor, routerstate, mimetype,
                    extension):
        if self.isdone is False:
            # Compose a single sting from function arguments:
            #   - carvpath : Child entity carvpath
            #   - nextactor : Name of the next actor that should receive this
            #                 child as job.
            #   - routerstate : String containing router state to be used by
            #                   FIVES style distributed router logic in next
            #                   worker to process this child.
            #   - mimetype : Communicate data mime-type with next actor.
            #   - extension : The next worker to receive this job will get
            #                 this job with this file extension for the
            #                 carvpath.
            val = (carvpath + ";" + nextactor + ";" + routerstate + ";" +
                   mimetype + ";" + extension)
            print "submit_child (",val,")"
            self.ctl["user.submit_child"] = val.encode()
            self.newdata = None

    # Mark the job as done without specifying a new target. This should close
    # and flush the provenance log for the toolchain this jib belongs to.
    def done(self):
        self.forward("", "")

    # Mark job as done and explicitly forward the job to an other module.
    def forward(self, nextactor, routerstate):
        if self.isdone is False:
            # Compose single string from function arguments.
            # - nextactor: Name of the next actor that should receive this
            #              a copy of this job next in this toolchain.
            # - routerstate: String containing router state to be uses by
            #                FIVES style distributed router logic in next
            #                worker to process the next incarnation of this
            #                job in the current toolchain.
            self.ctl["user.routing_info"] = nextactor + ";" + routerstate
            self.isdone = True


# Per worker context object.
class _Context:
    def __init__(self, mountpoint, actor, context, initial_sort_policy):
        # Regular expressions for validating select policy strings
        self.selectre = re.compile(r'^[SVDWC]{1,5}$')
        self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')
        self.mountpoint = mountpoint
        # MattockFS mountpoint level attributes.
        self.main_ctl = xattr.xattr(self.mountpoint + "/mattockfs.ctl")
        self.context = context
        # Named actor level for this worker as actor.
        self.actor_ctl = xattr.xattr(self.mountpoint + "/actor/" + actor +
                                     ".ctl")
        # Register as worker with MattockFS
        self.worker_ctl = None
        path = self.actor_ctl["user.register_worker"]
        self.worker_ctl = xattr.xattr(self.mountpoint + "/" + path)
        # Set job select policy if supplied as constructor argument
        if initial_sort_policy is not None:
            self.set_job_select_policy(policy=initial_sort_policy)

    # RAIIish unregister for MattockFS registered worker.
    def __del__(self):
        if self.worker_ctl is not None:
            try:
                self.worker_ctl["user.unregister"] = "1"
            except:
                pass

    # Set the select policy string for jobs
    def set_job_select_policy(self, policy):
        ok = bool(self.sortre.search(policy))  # Validate against regex.
        if ok:
            self.worker_ctl["user.job_select_policy"] = policy
        else:
            raise RuntimeError("Invalid sort policy string")

    # Only for load balancers: set module select policy string
    def set_actor_select_policy(self, policy):
        ok = bool(self.selectre.search(policy))
        if ok:
            self.worker_ctl["user.actor_select_policy"] = policy
        else:
            raise RuntimeError("Invalid select policy string")

    # Fetch the next job according to select policy or return None if
    # non available.
    def poll_job(self):
        try:
            job = self.worker_ctl["user.accept_job"]
        except:
            return None
        return _Job(mp=self.mountpoint,
                    job_ctl=self.mountpoint + "/" + job,
                    context=self.context)

    # Get the next job, if non is available, keep polling until one is.
    def get_job(self):
        while True:
            job = self.poll_job()
            if job is None:
                sleep(0.05)
            else:
                yield job

    # Set the weight for the actor itself. Used in selecting a module anycast
    # set for use in load balancing.
    def actor_set_weight(self, weight):
        self.actor_ctl["user.weight"] = str(weight)

    # Get the currently set weight for actor.
    def actor_get_weight(self):
        return int(self.actor_ctl["user.weight"])

    # Load balancing will only act on anycast sets larger than this treshold.
    def actor_set_overflow(self, overflow):
        self.actor_ctl["user.overflow"] = str(overflow)

    def actor_get_overflow(self):
        return int(self.actor_ctl["user.overflow"])

class LongPathMap:
    def __init__(self,mp):
        ctlpath = mp + "/mattockfs.ctl"
        self.cproot = mp + "/carvpath/"
        if not (os.path.isfile(ctlpath)):
            raise RuntimeError("File-system not mounted at "+mp)
        self.main_ctl = xattr.xattr(ctlpath)
    def __getitem__(self, i):
        cpath = self.cproot + i + ".dat"
        cp_ctl = xattr.xattr(cpath)
        return cp_ctl["user.long_path"]
    def __setitem__(self, i, val):
        print "add_longpath",val
        self.main_ctl["user.add_longpath"] = val
    def __contains__(self, key):
        cpath = self.cproot + i + ".dat"
        return os.path.isfile(cpath)

# Representation of a MattockFS mountpoint.
class MountPoint:
    def __init__(self, mp):
        self.context = carvpath.Context(LongPathMap(mp))
        self.mountpoint = mp
        ctlpath = self.mountpoint + "/mattockfs.ctl"
        if not os.path.isfile(ctlpath):
            raise RuntimeError("File-system not mounted at "+mp)
        self.main_ctl = xattr.xattr(ctlpath)

    # Register currnt process as a worker with the given actor/module name.
    def register_worker(self, modname, initial_policy=None):
        return _Context(self.mountpoint, modname, self.context,
                        initial_policy)

    # Request the fadvise status for the whole underlying archive file.
    def fadvise_status(self):
        st = self.main_ctl["user.fadvise_status"].split(";")
        return {"normal": int(st[0]), "dontneed": int(st[1])}

    # Request a CarvPathFile object  for the archive as a whole.
    def full_archive(self):
        return _CarvPathFile(self.mountpoint,
                             self.main_ctl["user.full_archive"], self.context)

    # Directly designate a carvpath.
    def __getitem__(self, cp):
        return _CarvPathFile(self.mountpoint, "carvpath/"+cp, self.context)

    # Request the number of workers currently active for named actor/module.
    def worker_count(self, actorname):
        actor_inf = xattr.xattr(self.mountpoint + "/actor/" + actorname +
                                ".inf")
        return int(actor_inf["user.worker_count"])

    # Request basic stats on the status of the anycast set for a named actor.
    def anycast_status(self, actorname):
        actor_inf = xattr.xattr(self.mountpoint + "/actor/" + actorname +
                                ".inf")
        st = actor_inf["user.anycast_status"].split(";")
        return {"set_size": int(st[0]), "set_volume": int(st[1])}

    # Force-unregister all active workers for a given actor/module.
    def actor_reset(self, actorname):
        actor_ctl = xattr.xattr(self.mountpoint + "/actor/" + actorname +
                                ".ctl")
        actor_ctl["user.reset"] = "1"

    # Request the current carvpath for the archive as a whole.
    def full_path(self, entity, ext="dat"):
        return self.mountpoint + "/carvpath/" + str(entity) + "." + ext


#Some dumb and inadequate versions of module framework components. There are there basically to allow
#seperate development of the real component. 


#Trivial router that routs data to single tool toolchain based soly on mime-type.
class TrivialRouter:
    def __init__(self):
        #Simple json that maps from mime-type to module;ext
        with open("/etc/mattock_trivial_router_conf.json","r") as f:
            jsondata = f.read()
            self.rules = json.loads(jsondata) 
    def _mime_to_module(self,mime):
        if mime in self.rules:
            rule = self.rules[mime]
            return rule[0:rule.find(";")]
        return None
    def _mime_to_ext(self,mime):
        if mime in self.rules:
            rule = self.rules[mime]
            return rule[rule.find(";")+1:]
        return "dat"
    def process_child_meta(self,meta):
        mime = meta["mime-type"]
        return self._mime_to_module(mime),"",mime,self._mime_to_ext(mime),None,""
    def set_state(self,router_state):
        pass
    def get_walk_argument(self):
        return None
    def process_parent_meta(self,toplevel_meta):
        pass
    def get_parentmeta_routing_info(self):
        return None,""
    def clear_state(self):
        pass
    def get_parentdata_routing_info(self):
        return None,""


class JsonSerializer:
    def __call__(self,meta):
       return json.dumps(meta) 

class DummyThrottler:
    def set_global_functors(self,fadvise_status,anycast_status):
        self.fadvise_status = fadvise_status
        self.anycast_status = anycast_status
    def on_anycast(self,meta_nexthop):
        pass
    def on_alloc(self,size):
        pass
    def set_worker(self,worker):
        pass


class TrivialTreeWalker:
    def __init__(self,module=None):
        self.module = module
    def set_module(self,module):
        self.module = module
    def mimetype(self,cp):
        return "bogus/bogus" #FIXME
    def _node_walk(self,node,child_submit,allocate_storage,job):
        for childnode in node.children():
            self._node_walk(childnode,child_submit,allocate_storage,job)
            cp = childnode.get_carvpath(allocate_storage)
            meta = childnode.get_meta()
            if not "mime-type" in meta:
                meta["mime-type"] = self.mimetype(cp)
            child_submit(job,cp,meta)
    def walk(self,carvpath,argument,child_submit,allocate_storage,job):
        node = self.module.root(carvpath,argument)
        self._node_walk(node,child_submit,allocate_storage,job) 
        return node.get_meta()

 

#This class is meant for binding together the low level MattockFS language bindings with higher level
#module framework components and the actual module that uses a higher level API. The EventLoop will
#poll all of the active CarvPath mounts and will comunicate with appropriate user supplied framework
#components in order to allow modules using such a module frameworj to process incomming jobs using a
#higher level API.
class EventLoop:
    def __init__(self,modname, module, router=TrivialRouter(), serializer=JsonSerializer(), throttler=DummyThrottler(), treewalker=TrivialTreeWalker(), initial_policy=None):
        treewalker.set_module(module)
        jsonfile = "/etc/mattockfs.json"
        self.count = 0
        with open(jsonfile,"r") as f:
            jsondata = f.read()
            data = json.loads(jsondata)
            self.count=data["instance_count"]
        self.mountpoints = []
        self.workers = []
        for mpno in range(0,self.count):
            mppath = "/var/mattock/mnt/" + str(mpno)
            self.mountpoints.append(MountPoint(mppath))
        for mp in self.mountpoints:
            self.workers.append(mp.register_worker(modname,initial_policy))
        self.router = router
        self.serializer = serializer
        self.throttler = throttler
        self.treewalker = treewalker
        self.throttler.set_global_functors(self._fadvise_status,self._anycast_status)
    def _fadvise_status(self):
        dontneed = 0
        normal = 0
        for mp in self.mountpoints:
            obj = mp.fadvise_status()
            dontneed = dontneed + obj["dontneed"]
            normal = normal + obj["normal"]
        return  {"normal": normal, "dontneed": dontneed}
    def _anycast_status(self, actorname):
        setsize = 0
        setvolume = 0
        for mp in self.mountpoints:
            obj = mp.anycast_status(actorname)
            setsize = setsize + obj["set_size"]
            setvolume = setvolume + obj["set_volume"]
        return  {"set_size": setsize, "set_volume": setvolume}
    def _get_job(self):
        sleepcount = 0 
        while True:
            for index in  range(0,self.count):
                worker=self.workers[index]
                job = worker.poll_job()
                if job is None:
                    sleepcount = sleepcount + 1
                else:
                    sleepcount = 0
                    yield job,worker
                if sleepcount == self.count:
                    sleep(0.05)           
    def _child_submit(self,job,carvpath,meta):
        data_nexthop,data_routerstate,data_mimetype,data_ext,meta_nexthop,meta_routerstate=self.router.process_child_meta(meta)
        if meta_nexthop != None:
            self.throttler.on_anycast(meta_nexthop)
            metablob = self.serializer(meta)
            mutable = job.childdata(len(metablob))
            with open(mutable, "r+") as f:
                f.seek(0)
                f.write(metablob)
            meta_carvpath = job.frozen_childdata()
            job.childsubmit(carvpath=meta_carvpath,
                        nextactor=meta_nexthop,
                        routerstate=meta_routerstate,
                        mimetype=self.serializer.mimetype,
                        extension=self.serializer.ext)
        if data_nexthop != None:
            self.throttler.on_anycast(data_nexthop)
            job.childsubmit(carvpath=carvpath,
                        nextactor=data_nexthop,
                        routerstate=data_routerstate,
                        mimetype=data_mimetype,
                        extension=data_ext)
    def _allocate_storage(self,size):
        self.throttler.on_alloc(size)
        return self.job.childdata(size)
    def __call__(self):
        for job,worker in self._get_job():
            self.job=job
            self.throttler.set_worker(worker)
            self.router.set_state(job.router_state)
            toplevel_meta = self.treewalker.walk(job.carvpath,self.router.get_walk_argument(),self._child_submit,self._allocate_storage,job)
            self.router.process_parent_meta(toplevel_meta)
            meta_module,meta_router_state = self.router.get_parentmeta_routing_info()
            if meta_module != None:
                metablob = self.serializer(toplevel_meta)
                mutable = job.childdata(len(metablob))
                with open(mutable, "r+") as f:
                    f.seek(0)
                    f.write(metablob)
                meta_carvpath = job.frozen_childdata()
                job.childsubmit(carvpath=meta_carvpath,
                            nextactor=meta_module,
                            routerstate=meta_router_state,
                            mimetype=self.serializer.mimetype,
                            extension=self.serializer.ext)
            data_module,data_router_state = self.router.get_parentdata_routing_info()
            if data_module == None:
                job.done()
            else:
                job.forward(data_module,data_router_state)
            self.router.clear_state()         


