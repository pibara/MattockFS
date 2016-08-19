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
from time import sleep
import carvpath
import longpathmap


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
            self.ctl["user.submit_child"] = val
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


# Representation of a MattockFS mountpoint.
class MountPoint:
    def __init__(self, mp):
        self.context = carvpath.Context(longpathmap.LongPathMap())
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
