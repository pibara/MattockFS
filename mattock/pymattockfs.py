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

import fuse
import anycast
import stat
import errno
import random
import re
import carvpath
import repository
import merkletree
import opportunistic_hash
import sys
import copy
import os
import redislongpathmap as longpathmap
import pwd
import json

fuse.fuse_python_api = (0, 2)


# A few constant values for common inode types.
STAT_MODE_DIR = (stat.S_IFDIR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP |
                 stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
STAT_MODE_DIR_NOLIST = (stat.S_IFDIR | stat.S_IXUSR | stat.S_IXGRP |
                        stat.S_IXOTH)
STAT_MODE_LINK = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP |
                  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
STAT_MODE_FILE = (stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |
                  stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
STAT_MODE_FILE_RO = stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH


# Generate a decent stat object.
def defaultstat(mode=STAT_MODE_DIR_NOLIST, size=0):
    st = fuse.Stat()
    st.st_blksize = 512
    st.st_mode = mode
    st.st_nlink = 1
    st.st_uid = os.geteuid()
    st.st_gid = os.getegid()
    st.st_size = size
    st.st_blocks = 0
    st.st_atime = 0
    st.st_mtime = 0
    st.st_ctime = 0
    return st

# Below is a set of helper structuring objects for different node types.
# There are all constructed from the parsepath method.


# Invalid non existing entity, returns ENOENT on all methods.
class NoEnt:  # pragma: no cover
    def getattr(self):
        return -errno.ENOENT

    def opendir(self):
        return -errno.ENOENT

    def readlink(self):
        return -errno.ENOENT

    def listxattr(self):
        return -errno.ENOENT

    def getxattr(self, name, size):
        return -errno.ENOENT

    def setxattr(self, name, val):
        return -errno.ENOENT

    def open(self, flags, path):
        return -errno.ENOENT


# The file-system top directory.
class TopDir:
    def getattr(self):  # pragma: no cover
        return defaultstat(STAT_MODE_DIR)

    def opendir(self):  # pragma: no cover
        return 0

    def readdir(self):  # pragma: no cover
        yield fuse.Direntry("mattockfs.ctl")
        yield fuse.Direntry("carvpath")
        yield fuse.Direntry("actor")
        yield fuse.Direntry("worker")
        yield fuse.Direntry("job")
        yield fuse.Direntry("mutable")
        yield fuse.Direntry("etc")

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return []

    def getxattr(self, name, size):  # pragma: no cover
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# One of the non listable directories.
class NoList:  # pragma: no cover
    def getattr(self):
        return defaultstat(STAT_MODE_DIR)

    def opendir(self):
        return -errno.EPERM

    def readlink(self):
        return -errno.EINVAL

    def listxattr(self):
        return []

    def getxattr(self, name, size):
        return -errno.ENODATA

    def setxattr(self, name, val):
        return -errno.ENODATA

    def open(self, flags, path):
        return -errno.EPERM

class ActorDir: 
    def __init__(self,actors):
        self.actors=actors

    def getattr(self):
        return defaultstat(STAT_MODE_DIR)

    def readdir(self):  # pragma: no cover
        for actor in self.actors.actors:
            yield fuse.Direntry(actor + ".ctl")
            yield fuse.Direntry(actor + ".inf")

    def opendir(self):
        return 0

    def readlink(self):
        return -errno.EINVAL

    def listxattr(self):
        return []

    def getxattr(self, name, size):
        return -errno.ENODATA

    def setxattr(self, name, val):
        return -errno.ENODATA

    def open(self, flags, path):
        return -errno.EPERM

class EtcDir:
    def getattr(self):
        return defaultstat(STAT_MODE_DIR)

    def readdir(self):  # pragma: no cover
        for entry in os.listdir("/etc/mattockfs.d"):
            if os.path.isfile("/etc/mattockfs.d/" + entry ):
                yield fuse.Direntry(entry)

    def opendir(self):
        return 0

    def readlink(self):
        return -errno.EINVAL

    def listxattr(self):
        return []

    def getxattr(self, name, size):
        return -errno.ENODATA

    def setxattr(self, name, val):
        return -errno.ENODATA

    def open(self, flags, path):
        return -errno.EPERM
 

# Top level mattockfs.ctl control file.
class TopCtl:
    def __init__(self, rep, context):
        self.rep = rep
        self.context = context

    def getattr(self):
        return defaultstat(STAT_MODE_FILE_RO)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return ["user.fadvise_status",
                "user.full_archive",
                "user.add_longpath"]

    def getxattr(self, name, size):
        if name == "user.fadvise_status":
            # Get fadvise info from the repository.
            return ";".join(map(lambda x: str(x),
                                self.rep.getTopThrottleInfo()))
        if name == "user.full_archive":
            # Get the current full archive carvpath from the repository.
            return "carvpath/" + str(self.rep.top.topentity) + ".raw"
        if name == "user.add_longpath":
            return ""
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        if name in ("user.fadvise_status",
                    "user.full_archive"):
            return -errno.EPERM
        if name == "user.add_longpath":
            val = val.split("carvpath/")[-1].split(".")[0]
            altval = str(self.context.parse(val))
            return 0
        return -errno.ENODATA
    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# A valid actor control file under $MP/actor/
class ActorCtl:
    def __init__(self, mod, fs):
        self.mod = mod
        self.fs = fs

    def getattr(self):
        return defaultstat(STAT_MODE_FILE_RO)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return ["user.weight",
                "user.overflow",
                "user.reset",
                "user.register_worker"]

    def getxattr(self, name, size):
        if name == "user.weight":
            # Actor object attribute
            return str(self.mod.weight)
        if name == "user.overflow":
            # Actor object overflow attribute
            return str(self.mod.overflow)
        if name == "user.reset":
            return "0"
        if name == "user.register_worker":
            if size == 0:
                # Don't register on listxattr !
                return 82
            else:
                # Only register a new worker on getxattr
                context = self.fs.GetContext()
                user = pwd.getpwuid(context["uid"])[0]
                path = "/proc/" + str(context["pid"]) + "/cmdline"
                cmd = []
                with open(path, "r") as f:
                    cmd = f.read().split("\0")[:-1]
                rval = ("worker/" +
                        self.mod.register_worker(user=user, command=cmd, pid=context["pid"]) +
                        ".ctl")
                return rval
        return -errno.ENODATA

    def setxattr(self, name, val):
        if name == "user.weight":
            try:
                asnum = int(val)
            except ValueError:
                return 0
            # Set weight for actor.
            self.mod.weight = asnum
            return 0
        if name == "user.overflow":
            try:
                asnum = int(val)
            except ValueError:
                return 0
            # Set overflow for actor.
            self.mod.overflow = asnum
            return 0
        if name in ("user.register_worker"):
            return -errno.EPERM
        if name == "user.reset":
            if val == "1":
                # Force-nregister all of the actor's active workers.
                self.mod.reset()
            return 0
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# Version of the actor control with reduced priviledges.
# Meant for use by workers of other actors that need to do routing tasks.
class ActorInf:
    def __init__(self, mod):
        self.mod = mod

    def getattr(self):
        return defaultstat(STAT_MODE_FILE_RO)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return ["user.anycast_status", "user.worker_count"]

    def getxattr(self, name, size):
        if name == "user.anycast_status":
            return ";".join(map(lambda x: str(x), self.mod.throttle_info()))
        if name == "user.worker_count":
            return str(self.mod.worker_count())
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        if name in ("user.anycast_status", "user.worker_count"):
            return -errno.EPERM
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# Valid worker control-file under $MP/worker/
class WorkerCtl:
    def __init__(self, worker, sortre, selectre, tick):
        self.worker = worker
        self.sortre = sortre
        self.selectre = selectre
        self.tick = tick
        actorname = worker.actorname
        self.kickstart_ok = False
        self.loadbalance_ok = False
        with  open("/etc/mattockfs.json") as config_file:
            conf = json.loads(config_file.read())
            if "thin_air_jobs" in conf and actorname in conf["thin_air_jobs"]:
               self.kickstart_ok = True
            if "steal_jobs"  in conf and actorname in conf["steal_jobs"]:
               self.loadbalance_ok = True
            
    def getattr(self):
        return defaultstat(STAT_MODE_FILE_RO)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        if self.loadbalance_ok == True :
            return ["user.job_select_policy",
                    "user.actor_select_policy",
                    "user.unregister",
                    "user.accept_job"]
        else:
            return ["user.job_select_policy",
                    "user.unregister",
                    "user.accept_job"]

    def getxattr(self, name, size):  # pragma: no cover
        if name == "user.job_select_policy":
            return self.worker.job_select_policy
        if name == "user.actor_select_policy":
            if self.loadbalance_ok == True:
                return self.worker.module_select_policy
            else:
                return -errno.ENODATA
        if name == "user.unregister":
            return "0"
        if name == "user.accept_job":
            if size == 0:
                # Don't accidently accept jobs with listxattr.
                return 77
            else:
                # Accept a new job for worker and tranfer responsibility to
                # worker.
                job = self.worker.accept_job()
                if job is None:
                    return -errno.ENODATA
                self.tick()
                return "job/" + job + ".ctl"
        return -errno.ENODATA

    def setxattr(self, name, val):
        if name == "user.job_select_policy":
            if self.kickstart_ok == False:
                val = re.sub('[kK]', '', val)
            ok = bool(self.sortre.search(val))
            if ok:
                # Set the job select pollicy for this worker.
                self.worker.job_select_policy = val
            return 0
        if name == "user.actor_select_policy":
            if self.loadbalance_ok == True:
                ok = bool(self.selectre.search(val))
                if ok:
                    # For a load balancer: set the module select policy.
                    self.worker.module_select_policy = val
                return 0
            else:
                return -errno.ENODATA
        if name == "user.unregister":
            if val == "1":
                # Unregister as worker. A worker should do this prior
                # to process exit!
                self.worker.unregister()
            return 0
        if name == "user.accept_job":  # pragma: no cover
            return -errno.EPERM
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# Valid job control-file under $MP/job/
class JobCtl:
    def __init__(self, job, tick):
        self.job = job
        self.tick = tick

    def getattr(self):
        return defaultstat(STAT_MODE_FILE_RO)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return ["user.routing_info",
                "user.submit_child",
                "user.allocate_mutable",
                "user.frozen_mutable",
                "user.current_mutable",
                "user.job_carvpath"]

    def getxattr(self, name, size):
        if name == "user.routing_info":
            # Compose routing info string from actor name, router state
            # and mime_type.
            print self.job
            print self.job.actorname
            print self.job.router_state
            print self.job.mime_type
            return (self.job.actorname + ";" + self.job.router_state +
                    ";" + self.job.mime_type)
        if name == "user.submit_child":  # pragma: no cover
            return ""
        if name == "user.allocate_mutable":  # pragma: no cover
            return ""
        if name == "user.current_mutable":
            rval = self.job.get_mutable()
            if rval is None:
                return ""
            return "mutable/" + rval + ".dat"
        if name == "user.frozen_mutable":
            if size == 0:
                # Don't accidentaly freeze a mutable with listxattr
                return 81
            # Freeze mutable, updates no longer permitted, sparse cap
            # invalidated.
            frozen = self.job.get_frozen_mutable()
            if frozen is not None:
                return "carvpath/" + self.job.get_frozen_mutable() + ".dat"
            return -errno.ENODATA
        if name == "user.job_carvpath":
            # Get the carvapth of the job data with extension as indicated by
            # worker that initiated the tool chain.
            return ("carvpath/" +
                    self.job.carvpath + "." + self.job.file_extension)
        return -errno.ENODATA

    def setxattr(self, name, val):
        if name == "user.routing_info":
            if ";" in val:
                parts = val.split(";")
                if self.job.actors.validactorname(parts[0]):
                    # Mark job as done for this module and forward to next
                    # actor in tool chain.
                    self.job.next_hop(actor=parts[0], state=parts[1])
            return 0
        if name == "user.submit_child":
            parts = val.split(";")
            if len(parts) == 5:
                # carvpath, nexthop, routerstate, mime, ext
                self.job.submit_child(carvpath=parts[0], nexthop=parts[1],
                                      routerstate=parts[2], mimetype=parts[3],
                                      extension=parts[4])
                self.tick()
            return 0
        if name == "user.allocate_mutable":
            # Create a mutable of the given size.
            self.job.create_mutable(msize=int(val))
            return 0
        if name in ("user.frozen_mutable",
                    "user.job_carvpath"):  # pragma: no cover
            return -errno.EPERM
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM


# A valid mutable data temporary under $MP/mutable
class MutableCtl:
    def __init__(self, carvpath, rep, context):
        self.rep = rep
        self.carvpath = carvpath
        self.context = context

    def getattr(self):
        # Return base stat with proper size attribute.
        size = self.context.parse(self.carvpath).totalsize
        return defaultstat(STAT_MODE_FILE_RO, size)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        return []

    def getxattr(self, name, size):  # pragma: no cover
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        return -errno.ENODATA

    def open(self, flags, path):
        # Register path and carvpath as opened.
        return self.rep.open(carvpath=self.carvpath, path=path)


# Normal CarvPath file under $MP/carvath/
class CarvPathFile:
    def __init__(self, carvpath, rep, context, actors):
        self.carvpath = carvpath
        self.rep = rep
        self.context = context
        self.actors = actors
        # Can't open a file with any of these flags set.
        self.badflags = (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT |
                         os.O_TRUNC | os.O_EXCL)

    def getattr(self):
        # Return standard read only file stat with proper size attribute.
        size = self.context.parse(self.carvpath).totalsize
        return defaultstat(STAT_MODE_FILE_RO, size)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):  # pragma: no cover
        return -errno.EINVAL

    def listxattr(self):  # pragma: no cover
        #Only list user,long_path if a 'D'igest carvpath.
        if self.carvpath.split("carvpath/")[-1].split(".")[0][0] == "D":
            return ["user.opportunistic_hash",
                    "user.fadvise_status",
                    "user.force_fadvise",
                    "user.long_path"]
        else:
            return ["user.opportunistic_hash",
                    "user.fadvise_status",
                    "user.force_fadvise"]

    def getxattr(self, name, size):
        if name == "user.opportunistic_hash":
            offset = "0"
            hashresult = ""
            # Return opportunistic hashing info if carvpath in any active
            # toolchain.
            if self.carvpath in self.actors.rep.stack.ohashcollection.ohash:
                ohash = self.actors.rep.stack.ohashcollection.ohash[
                          self.carvpath].ohash
                offset = str(ohash.offset)
                hashresult = ohash.result
            return hashresult + ";" + offset
        if name == "user.fadvise_status":
            # Return fadvise status for this single carvpath.
            return ";".join(map(lambda x: str(x),
                                self.actors.rep.stack.carvpath_fadvise_info(
                                  carvpath=self.carvpath)))
        if name == "user.force_fadvise":
             return -errno.EPERM
        if name == "user.long_path":
            carvpath=self.carvpath.split("carvpath/")[-1].split(".")[0]
            return self.context.longpathmap[carvpath]
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        if name == "user.force_fadvise":
            if self.rep.stack.carvpath_force_fadvise(self.carvpath,val) == True:
                return 0
            return -errno.EINVAL 
        if name in ("user.opportunistic_hash",
                    "user.fadvise_status",
                    "user.long_path"):
            return -errno.EPERM
        return -errno.ENODATA

    def open(self, flags, path):
        # Make sure file is opened read-only
        if (flags & self.badflags) != 0:
            return -errno.EPERM
        # Register file as opened with the repository.
        return self.rep.open(self.carvpath, path)


# For two (and more) level CarvPaths, symbolic link at level two.
class CarvPathLink:
    def __init__(self, cp, ext):
        if ext is None:
            self.link = "../" + cp
        else:
            self.link = "../" + cp + "." + ext

    def getattr(self):
        return defaultstat(STAT_MODE_LINK)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):
        return self.link

    def listxattr(self):  # pragma: no cover
        return []

    def getxattr(self, name, size):  # pragma: no cover
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM

class EtcLink:
    def __init__(self,entity,rep):
        self.entity=entity
        self.rep = rep
        self.link = "/etc/mattockfs.d/" + entity
    def getattr(self):
        return defaultstat(STAT_MODE_FILE)

    def opendir(self):  # pragma: no cover
        return -errno.ENOTDIR

    def readlink(self):
        return self.link

    def listxattr(self):  # pragma: no cover
        return ["user.snapshot"]

    def getxattr(self, name, size):  # pragma: no cover
        if name == "user.snapshot":
            with open(self.link,"r") as f:
                return self.rep.snapshot(f.read())
        return -errno.ENODATA

    def setxattr(self, name, val):  # pragma: no cover
        if name == "user.snapshot":
            return -errno.EPERM
        return -errno.ENODATA

    def open(self, flags, path):  # pragma: no cover
        return -errno.EPERM

# The actual FUSE MattockFS file-system.
class MattockFS(fuse.Fuse):
    def __init__(self, dash_s_do, version, usage, dd, lpdb, journal,
                 provenance_log, ohash_log, refcount_log, mtlog):
        super(MattockFS, self).__init__(version=version, usage=usage,
                                        dash_s_do=dash_s_do)
        self.longpathdb = lpdb
        self.context = carvpath.Context(lpmap=self.longpathdb)
        self.topdir = TopDir()
        self.nolistdir = NoList()
        # Regular expressions for select policies.
        self.selectre = re.compile(r'^[SVDWC]{1,5}$')
        self.sortre = re.compile(r'^(K|[RrOHDdWS]{1,6})$')
        self.archive_dd = dd
        self.mtlog = mtlog = merkletree.MerkleTreeLog(mtlog)
        self.rep = repository.Repository(
            reppath=self.archive_dd,
            context=self.context,
            ohash_log=ohash_log,
            refcount_log=refcount_log,
            mtlog = self.mtlog)
        self.ms = anycast.Actors(
            rep=self.rep,
            journal=journal,
            provenance=provenance_log,
            context=self.context,
            stack=self.rep.stack,
            col=self.rep.col)
        self.etcdir = EtcDir()
        self.topctl = TopCtl(rep=self.rep, context=self.context)
        self.actordir = ActorDir(actors=self.ms)
        self.needinit = True
    # Helper used by multiple fuse hooks to create one of the node type
    # objects above.
    def parsepath(self, path):
        if path == "/":
            return self.topdir
        tokens = path[1:].split("/")
        # MattockFS goes 3 levels deep at most.
        if len(tokens) > 3:
            return None
        # These are the only possible level one token values.
        if tokens[0] in ("carvpath",
                         "actor",
                         "worker",
                         "job",
                         "mutable",
                         "mattockfs.ctl",
                         "etc"):
            # Only the carvpath directory does to 3 levels deep.
            if len(tokens) > 2 and tokens[0] != "carvpath":
                return NoEnt()
            if len(tokens) == 1:
                if tokens[0] == "mattockfs.ctl":
                    return self.topctl
                if tokens[0] == "actor":
                    return self.actordir
                if tokens[0] == "etc":
                    return self.etcdir
                # All other level 1 directories are unlistable.
                return self.nolistdir
            if len(tokens) == 2 and tokens[0] == "etc":
                return EtcLink(tokens[1],self.rep)
            if tokens[0] == "carvpath":
                lastpart = tokens[1].split(".")
                # At most one dot in valid carvpath.
                if len(lastpart) > 2:
                    return NoEnt()
                topcp = lastpart[0]
                # The level 2 token (without extension) must be valid carvpath
                if not self.rep.validcarvpath(cp=topcp):
                    return NoEnt()
                if len(tokens) == 2:
                    if len(lastpart) == 2:
                        # Anything with an extension is a file.
                        return CarvPathFile(carvpath=topcp,
                                            rep=self.rep,
                                            context=self.context,
                                            actors=self.ms)
                    # Without an extension its a directory for a multi level
                    # carvpath annotation to be resolved.
                    return self.nolistdir
                # must be 3 now
                lastpart = tokens[2].split(".")
                # Again, only one dot in a valid carvpath.
                if len(lastpart) > 2:
                    return NoEnt()
                ext = None
                if len(lastpart) == 2:
                    ext = lastpart[1]
                # Flatten the multi-level carvpath into a single level one.
                link = self.rep.flatten(basecp=topcp, subcp=lastpart[0])
                if link is not None:
                    return CarvPathLink(cp=link, ext=ext)
                return NoEnt()
            lastpart = tokens[1].split(".")
            if len(lastpart) != 2:
                return NoEnt()
            handle = lastpart[0]
            extension = lastpart[1]
            if extension == "ctl":
                if tokens[0] == "actor":
                    if self.ms.validactorname(actorname=handle):
                        return ActorCtl(mod=self.ms[handle], fs=self)
                    return NoEnt()
                if tokens[0] == "worker":
                    if self.ms.validworkercap(handle=handle):
                        return WorkerCtl(worker=self.ms.workers[handle],
                                         sortre=self.sortre,
                                         selectre=self.selectre,
                                         tick=self.ms.tick)
                    return NoEnt()
                if tokens[0] == "job":
                    if self.ms.validjobcap(handle=handle):
                        return JobCtl(job=self.ms.jobs[handle],tick=self.ms.tick)
                    return NoEnt()
                return NoEnt()
            if extension == "dat" and tokens[0] == "mutable":
                if self.ms.validnewdatacap(handle=handle):
                    return MutableCtl(carvpath=self.ms.newdata[handle],
                                      rep=self.rep,
                                      context=self.context)
            if extension == "inf" and tokens[0] == "actor":
                if self.ms.validactorname(actorname=handle):
                    # Reduced priviledge version of the actor ctl file.
                    return ActorInf(mod=self.ms[handle])
                return NoEnt()
            return NoEnt()
        return NoEnt()

    # Forward getattr to parsepath result.
    def getattr(self, path):
        return self.parsepath(path).getattr()

    # Do nothing on setattr.
    def setattr(self, path, hmm):
        return 0

    # Forward
    def opendir(self, path):
        return self.parsepath(path).opendir()

    # Forward
    def readdir(self, path, offset):
        return self.parsepath(path).readdir()

    # Do nothing on releasedir.
    def releasedir(self, path):
        return 0

    # Forward
    def readlink(self, path):
        return self.parsepath(path).readlink()

    # Forward
    def listxattr(self, path, huh):
        return self.parsepath(path).listxattr()

    def getxattr(self, path, name, size):
        rval = self.parsepath(path).getxattr(name, size)
        if isinstance(rval, int) and rval < 0:
            return rval
        if size == 0:
            # If field size is requested but forwarding yields string:
            # convert to size.
            if not isinstance(rval, int):
                rval = len(rval)
        return rval

    # Forward
    def setxattr(self, path, name, val, more):
        return self.parsepath(path).setxattr(name, val)

    def main(self, args=None):
        fuse.Fuse.main(self, args)

    # Forward
    def open(self, path, flags):
        rval = self.parsepath(path).open(flags, path)
        return rval

    # Forward open file operations to repository.

    def release(self, path, fh):
        return self.rep.close(path)

    def read(self, path, size, offset):
        return self.rep.read(path, offset, size)

    def write(self, path, data, offset):
        rval = self.rep.write(path, offset, data)
        return rval

    # We don't allow any truncating.
    def truncate(self, path, len, fh=None):
        return -errno.EPERM

    def flush(self, path):
        self.rep.flush()


# File-system startup
def run(mattockitem="0"):
    mattockdir = "/var/mattock"
    # Make sure the required directory structure exists for archive log and
    # mount.
    if not os.path.isdir(mattockdir):
        print ("ERROR: ",
               mattockdir,
               "should exist and be owned by the mattockfs user")
        sys.exit()
    for subdir in ["archive", "log", "mnt"]:
        sd = mattockdir + "/" + subdir
        if not os.path.isdir(sd):
            try:
                os.mkdir(sd)
            except:
                print ("ERROR: Can't create missing",
                       sd,
                       ";",
                       mattockdir,
                       "should be owned by the mattockfs user")
                sys.exit()
    # Now look at the specified mattock item (default "0" and its files)
    mp = mattockdir + "/mnt/" + mattockitem
    if not os.path.isdir(mp):
        try:
            os.mkdir(mp)
        except:
            print ("ERROR: Can't create missing",
                   mp,
                   ";",
                   mattockdir + "/mnt should be owned by the mattockfs user")
            sys.exit()
    # The raw archive that MattockFS runs on top of.
    dd = mattockdir + "/archive/" + mattockitem + ".dd"
    # Journal file and provenance log.
    journal = mattockdir + "/log/" + mattockitem + ".journal"
    provenance_log = mattockdir + "/log/" + mattockitem + ".provenance"
    # Log for opportunistic hashing results.
    ohash_log = mattockdir + "/log/" + mattockitem + ".ohash"
    # Debugging log for reference count logging.
    refcount_log = mattockdir + "/log/" + mattockitem + ".refcount"
    #Merkletree log
    merkletree_log = mattockdir + "/log/" + mattockitem + ".merkletree"
    # Mountpoint.
    mp = mattockdir + "/mnt/" + mattockitem
    sys.argv.append(mp)
    mattockfs = MattockFS(
                  version='%prog ' + '0.3.0',
                  usage='Mattock filesystem ' + fuse.Fuse.fusage,
                  dash_s_do='setsingle',
                  dd=dd,
                  lpdb=longpathmap.LongPathMap(),
                  journal=journal,
                  provenance_log=provenance_log,
                  ohash_log=ohash_log,
                  refcount_log=refcount_log,
                  mtlog=merkletree_log)
    mattockfs.parse(errex=1)
    mattockfs.flags = 0
    mattockfs.multithreaded = 0
    # Actually run the file system.
    mattockfs.main()

if __name__ == '__main__':
    run()
