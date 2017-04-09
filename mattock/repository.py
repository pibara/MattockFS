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
import errno
import copy
import os
import fcntl
import carvpath
import refcount_stack
import opportunistic_hash


try:
    from os import posix_fadvise, POSIX_FADV_DONTNEED, POSIX_FADV_NORMAL, POSIX_FADV_WILLNEED, POSIX_FADV_RANDOM, POSIX_FADV_NOREUSE, POSIX_FADV_SEQUENTIAL
except:  # pragma: no cover
    try:
        from fadvise import (posix_fadvise,
                             POSIX_FADV_DONTNEED,
                             POSIX_FADV_NORMAL,
                             POSIX_FADV_WILLNEED,
                             POSIX_FADV_RANDOM,
                             POSIX_FADV_NOREUSE,
                             POSIX_FADV_SEQUENTIAL)
    except:
        import sys
        print("")
        print("\033[93mERROR:\033[0m fadvise module not installed.")
        print("Please install fadvise python module.")
        print("Run:")
        print("")
        print("    sudo pip install fadvise")
        print("")
        sys.exit()


# Functor class for fadvise on an archive fd.
class _FadviseFunctor:
    def __init__(self, fd):
        self.fd = fd

    def __call__(self, offset, size, willneed):
        if willneed:
            posix_fadvise(self.fd, offset, size, POSIX_FADV_WILLNEED)
        else:
            posix_fadvise(self.fd, offset, size, POSIX_FADV_DONTNEED)
    def normal(self, offset,size):
        posix_fadvise(self.fd, offset, size, POSIX_FADV_NORMAL)
    def random(self, offset, size):
        posix_fadvise(self.fd, offset, size, POSIX_FADV_RANDOM)
    def sequential(self, offset, size):
        posix_fadvise(self.fd, offset, size, POSIX_FADV_SEQUENTIAL)
    def noreuse(self, offset, size):
        posix_fadvise(self.fd, offset, size, POSIX_FADV_NOREUSE)


# RAII class for keeping a file lock during sparse grow operations.
class _RaiiFLock:
    def __init__(self, fd):
        self.fd = fd
        fcntl.flock(self.fd, fcntl.LOCK_EX)

    def __del__(self):
        fcntl.flock(self.fd, fcntl.LOCK_UN)


# Class representing an open file. This can either be a mutable or a carvpath
# file.
class _OpenFile:
    def __init__(self, stack, cp, entity, fd, ohashcollection):
        self.cp = cp
        self.stack = stack
        self.ohashcollection = ohashcollection
        self.entity = entity
        self.fd = fd
        # Add the carvpath to the refcount stack.
        self.stack.add_carvpath(carvpath=cp)
        self.refcount = 1  # Note: This refcount is maintained by the
        #                        repository as not to border the refcount
        #                        stack with the same file twice.

    def __del__(self):
        # Remove from refcount stack on deletion.
        self.stack.remove_carvpath(carvpath=self.cp)

    def pread(self, chunk):
        # Read the chunk from offset, os.pread would be better but does not
        # exist in python 2.
        if chunk.issparse():
            return "\0" * chunk.size
        else:
            os.lseek(self.fd, chunk.offset, 0)
            return os.read(self.fd, chunk.size)

    def pwrite(self, chunk, chunkdata):
        # Write a chunk to the proper offset. os.pwrite would be better but
        # does not exist in python 2.
        os.lseek(self.fd, chunk.offset, 0)
        os.write(self.fd, chunkdata)
        return

    def read(self, offset, size):
        # Create an entity object for the thing we need to read.
        readent = self.entity.subentity(
          childent=carvpath._Entity(lpmap=self.entity.longpathmap,
                                    maxfstoken=self.entity.maxfstoken,
                                    fragments=[carvpath.Fragment(
                                        offset=offset,
                                        size=size)]),
          truncate=True)
        # Start with empty result.
        result = b''
        for chunk in readent:  # One entity chunk at a time.
            datachunk = self.pread(chunk=chunk)  # Read chunk from  offset
            result += datachunk  # Add chunk to result.
            if not chunk.issparse():
                self.ohashcollection.lowlevel_read_data(
                  offset=chunk.offset,
                  data=datachunk)  # Do opportunistic hasing
                  #                  if possible.
        return result

    def write(self, offset, data):
        size = len(data)
        # Create an entity object for the thing we need to write.
        writeent = self.entity.subentity(
          childent=carvpath._Entity(lpmap=self.entity.longpathmap,
                                    maxfstoken=self.entity.maxfstoken,
                                    fragments=[carvpath.Fragment(
                                        offset=offset,
                                        size=size)]),
          truncate=True)
        # Start of at a zero data index wigin our writale entity.
        dataindex = 0
        for chunk in writeent:  # One fragment at a time.
            # Get the part of our writable data we need for this fragment.
            chunkdata = data[dataindex:dataindex+chunk.size]
            dataindex += chunk.size  # Update dataindex for next fragment.
            # Write data fragment to propper offset
            self.pwrite(chunk=chunk, chunkdata=chunkdata)
            # Update opportunistic hasing if possible.
            self.ohashcollection.lowlevel_written_data(offset=chunk.offset,
                                                       data=chunkdata)
        return size


class Repository:
    def __init__(self, reppath, context, ohash_log, refcount_log):
        self.context = context
        # Create a new opportunistic hash collection.
        self.col = opportunistic_hash.OpportunisticHashCollection(
                         carvpathcontext=context,
                         ohash_log=ohash_log)
        # We start off with zero open files
        self.openfiles = {}
        # Open the underlying data file and create if needed.
        self.fd = os.open(reppath,
                          (os.O_RDWR |
                           os.O_LARGEFILE |
                           os.O_NOATIME |
                           os.O_CREAT))
        # Get the current repository total size.
        cursize = os.lseek(self.fd, 0, os.SEEK_END)
        # Set the entire repository as dontneed and assume everything to be
        # cold data for now.
        posix_fadvise(self.fd, 0, cursize, POSIX_FADV_DONTNEED)
        # Create CarvPath top entity of the proper size.
        self.top = self.context.make_top(size=cursize)
        # Create fadvise functor from fd.
        fadvise = _FadviseFunctor(fd=self.fd)
        # Create a referencecounting carvpath stack using our fadvise functor
        # and ohash collection.
        self.stack = refcount_stack.CarvpathRefcountStack(
              carvpathcontext=self.context,
              fadvise=fadvise,
              ohashcollection=self.col,
              refcount_log=refcount_log)

    def __del__(self):
        self.stack = None
        self.openfiles = None
        # On destruction close the underlying file.
        os.close(self.fd)

    def _grow(self, chunksize):
        # Use a file lock to atomically allocate a new chunk of
        # (at first sparse) file data.
        l = _RaiiFLock(fd=self.fd)
        cursize = os.lseek(self.fd, 0, os.SEEK_END)
        os.ftruncate(self.fd, cursize+chunksize)
        self.top.grow(chunk=cursize + chunksize - self.top.size)
        return cursize

    def snapshot(self,data):
        chunksize = len(data)
        offset = self._grow(chunksize=chunksize)
        os.lseek(self.fd, offset, 0)
        os.write(self.fd, data)
        cp = str(carvpath._Entity(lpmap=self.context.longpathmap,
                                  maxfstoken=self.context.maxfstoken,
                                  fragments=[
                                     carvpath.Fragment(offset=offset,
                                                       size=chunksize)]))
        return cp

    # It multiple instances of MattockFS use the same repository,
    # sync archive size with the underlying file size.
    def multi_sync(self):
        cursize = os.lseek(self.fd, 0, os.SEEK_END)
        grown = cursize - self.top.size
        self.top.grow(chunk=grown)
        return grown

    # Allocate a new piece of mutable data and return CarvPath
    def newmutable(self, chunksize):
        # Grow the underlying archive by chunksize
        chunkoffset = self._grow(chunksize=chunksize)
        # Get the (currently still secret) carvpath of the unfrozen allocated
        # chunk.
        cp = str(carvpath._Entity(lpmap=self.context.longpathmap,
                                  maxfstoken=self.context.maxfstoken,
                                  fragments=[
                                     carvpath.Fragment(offset=chunkoffset,
                                                       size=chunksize)]))
        return cp

    # Return the size of the part of the repository with a refcount > 0
    def volume(self):
        if len(self.stack.fragmentrefstack) == 0:
            return 0
        return self.stack.fragmentrefstack[0].totalsize

    # Check if a given carvpath is valid and possible within the repository
    # size.
    def validcarvpath(self, cp):
        try:
            ent = self.context.parse(path=cp)
            if self.top.test(child=ent):
                return True
            # If the Carvpath is out of range it could be an other system
            # in the NFS-meshup  grew the repository so just to be sure,
            # lets check for that.
            self.multi_sync()
            if self.top.test(child=ent):
                return True
            return False
        except:
            # Invalid format.
            return False

    # Flatten a two level carvpath.
    def flatten(self, basecp, subcp):
        try:
            ent = self.context.parse(path=basecp + "/" + subcp)
            return str(ent)
        except:
            return None

    # Get the volume of the combined job carvpaths in a named anycast set.
    def anycast_set_volume(self, anycast):
        volume = 0
        for jobid in anycast:
            cp = anycast[jobid].carvpath
            volume += self.context.parse(
                  path=anycast[jobid].carvpath).totalsize
        return volume

    # Get the most suitable entity from a given set according to given policy
    def anycast_best(self, anycast, sort_policy):
        if len(anycast) > 0:
            # Convert anycast set to a carvpath indexed map.
            cp2key = {}
            for anycastkey in anycast.keys():
                cp = anycast[anycastkey].carvpath
                cp2key[cp] = anycastkey
            # Ask the reference counting stack for the best carvpath for the
            # pollicy.
            bestcp = (
              self.stack.priority_custompick(params=sort_policy,
                                             intransit=cp2key.keys()).carvpath
              )
            # Return the anycast entry.
            return cp2key[bestcp]
        return None

    # Get fadvise info on the underlying repository file.
    def getTopThrottleInfo(self):
        totalsize = self.top.size
        normal = self.volume()
        dontneed = totalsize-normal
        return (normal, dontneed)

    # For use by loadbalancer; get best actor, if any, for load balancing job
    # selection.
    def anycast_best_actors(self, actorsstate, actorset, letter):
        # Only fetch the volume later on if V/C/D is in the policy.
        fetchvolume = False
        if letter in "VDC":
            fetchvolume = True
        # Start with empty set as best actors.
        bestactors = set()
        bestval = 0
        for actor in actorset:
            anycast = actorsstate.actors[actor].anycast
            # Only sets where the job count exceeds the overflow are canddates
            overflow = actorsstate.actors[actor].overflow
            if len(anycast) > overflow:
                # Fetch set volume if policy demands it.
                volume = 0
                if fetchvolume:
                    volume = self.anycast_set_volume(anycast=anycast)
                val = 0
                # Get the value to find the best actor with depending on
                # policy letter.
                if letter == "S":
                    val = len(anycast)  # Number of jobs in the set.
                if letter == "V":
                    val = volume        # Total carvpath volume of the set.
                if letter == "D":
                    if volume > 0:
                        # Jobs per byte of carvpath volume.
                        val = float(len(anycast))/float(volume)
                    else:
                        if len(anycast) > 0:
                            # Big number avoiding divide by zero
                            val = 100*len(anycast)
                if letter == "W":
                    # The weight of the actor
                    val = actorsstate[actor].weight
                if letter == "C":
                    if volume > 0:
                        # weight normalized jobs per byte of carvpath volume.
                        val = (float(actorsstate[actor].weight) *
                               float(len(anycast))/float(volume))
                    else:
                        if len(anycast) > 0:
                            # Big number avoiding divide by zero.
                            val = 100*len(anycast)*actorsstate[actor].weight
                if val > bestval:
                    # If the current value is better than the best value,
                    # update best and start with a new set of best actors.
                    bestval = val
                    bestactors = set()
                if val == bestval:
                    # If value is equal to best value, add actor to the set
                    # of best actors.
                    bestactors.add(actor)
        return list(bestactors)

    # Open a pseudo file within the repository.
    def open(self, carvpath, path):
        if path in self.openfiles:
            # If a copy of this file is already opened, increase the
            # reference count.
            self.openfiles[path].refcount += 1
        else:
            # Otherwise, parse the carvpath and create a new entry in the
            # openfiles map.
            ent = self.context.parse(path=carvpath)
            col = self.stack.ohashcollection
            self.openfiles[path] = _OpenFile(stack=self.stack,
                                             cp=carvpath,
                                             entity=ent,
                                             fd=self.fd,
                                             ohashcollection=col)
        return 0

    # Read data from an open file
    def read(self, path, offset, size):
        if path in self.openfiles:
            return self.openfiles[path].read(offset=offset, size=size)
        return -errno.EIO

    # Write data to an open file.
    def write(self, path, offset, data):
        if path in self.openfiles:
            return self.openfiles[path].write(offset=offset, data=data)
        return -errno.EIO

    def flush(self):
        return os.fsync(self.fd)

    # Close a file.
    def close(self, path):
        # Decrement refcount.
        self.openfiles[path].refcount -= 1
        # Only delete open file once refcount reaches zero.
        if self.openfiles[path].refcount < 1:
            del self.openfiles[path]
        return 0


if __name__ == "__main__":  # pragma: no cover
    import carvpath
    import opportunistic_hash
    context = carvpath.Context(lpmap={}, maxtokenlen=160)
    rep = Repository(reppath="/var/mattock/archive/0.dd",
                     context=context,
                     ohash_log="test3.log",
                     refcount_log="test4.log")
    entity = context.parse(path="1234+5678")
    f1 = rep.open(carvpath="1234+5678", path="/frozen/1234+5678.dat")
    print rep.volume()
