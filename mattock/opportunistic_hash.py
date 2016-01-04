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
import copy


try:
    from pyblake2 import blake2b
    ohash_algo = blake2b
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
try:  # pragma: no cover
    # When this was written, pyblake2 didn't implement blake2bp yet.
    # Hopefully it does in the future so the Python implementation can be
    # close to as fast as, and compatible with, the C++ implementation.
    from pyblake2 import blake2bp
    ohash_algo = blake2bp
except:  # pragma: no cover
    pass


# Opportunistic hashing state for a single fixed-size entity
class _Opportunistic_Hash:
    def __init__(self, size):
        # Initiate hashing algo state.
        self._h = ohash_algo(digest_size=32)
        # Start at offset 0.
        self.offset = 0
        self.isdone = False
        self.result = "INCOMPLETE-OPPORTUNISTIC_HASHING"
        self.fullsize = size
        # If this is a new mutable entity, start off as clean sparse.
        self.cleansparse = True

    # A sparse chunk
    def sparse(self, length, offset):
        # Process as a chunk of read zeroes.
        self.read_chunk(data=bytearray(length), offset=offset)

    # Indicate that mutable entity won't be written to any more times.
    def freeze(self):
        if self.cleansparse:
            # If we can assume clean sparse layout, process the remaining size
            # as sparse.
            lastchunksize = self.fullsize - self.offset
            self.sparse(length=lastchunksize, offset=self.offset)

    # Process a piece of data written to offset.
    def written_chunk(self, data, offset):
        if offset < self.offset:
            # Something written before our current hashing offset. This means
            # hashed data changed and we need to start over.
            self._h = ohash_algo(digest_size=32)  # reset hashing
            self.offset = 0  # Reset our hashing offset to zero.
            self.isdone = False
            self.cleansparse = False  # We can no longer assume we are a
            #                           cleanly sparse file.
            self.result = "INCOMPLETE-OPPORTUNISTIC_HASHING"
        # If writing a sparse file in sequence, we can forward our hashing
        # by assuming the file is meant to become sparse.
        if (offset > self.offset) and self.cleansparse:
            # There is a gap we can assume sparse.
            difference = offset - self.offset
            times = difference / 65536
            for i in range(0, times):
                self.sparse(length=65536, offset=self.offset)
            self.sparse(length=difference % 65536, offset=self.offset)
        # If the offset of the data alligns with the hashing offset, update
        # the hash with our new data.
        if offset == self.offset:
            self._h.update(data)
            self.offset += len(data)
        # If the offset equals the size, than complete the hashing process.
        if self.offset > 0 and self.offset == self.fullsize:
            self.done()

    # Process a piece of data read from offset.
    def read_chunk(self, data, offset):
        if ((not self.isdone) and offset <= self.offset and
           offset+len(data) > self.offset):
            # Fragment overlaps offset;Find part that we didn't process yet
            start = self.offset - offset
            datasegment = data[start:]
            self._h.update(datasegment)
            self.offset += len(datasegment)
            # If the offset equals size,than complete the hashing process.
            if self.offset > 0 and self.offset == self.fullsize:
                self.done()

    # Complete the hashing process.
    def done(self):
        if not self.isdone:
            self.result = self._h.hexdigest()
            self.isdone = True


# Class to represent a single opportunistic hashing candidate CarvPath.
class _OH_Entity:
    def __init__(self, entity, log):
        self.log = log
        self.ent = entity.copy()
        self.ohash = _Opportunistic_Hash(size=self.ent.totalsize)
        # The shifting range-of-interest for reading operations for CarvPath.
        self.roi = self.ent.getroi(from_offset=0)
        # Static range-of-interest for writing operations
        self.writeroi = self.ent.getroi(from_offset=0)

    # Process data; either written or read.
    def process_parent_chunk(self, data, parentoffset, writemode):
        # Calculate the parent ending offset.
        parentfragsize = len(data)
        parentendoffset = parentoffset+parentfragsize-1
        # Select the right range-of-interest depending on the type operation.
        if writemode:
            roi = self.writeroi
        else:
            roi = self.roi
        # Quick range of interest check. Only check if hashing is needed and
        # start of interest is contained in parent frag.
        if ((not self.ohash.isdone) and
           (parentoffset <= roi[0] or writemode) and
           parentoffset < roi[1] and
           parentendoffset > roi[0]):
            childoffset = 0  # Start of with a child offset of zero.
            working = False  # Marks if we are working on the hash.
            updated = False  # This indicates that the hash has been updated
            #                  in this read_parent_chunk invocation.
            wasdone = self.ohash.isdone
            # Look at the parent chunk for all the fragments that make up the
            # CarvPath entity.
            for fragment in self.ent.fragments:
                # First look at real fragments.
                if not fragment.issparse():
                    lastbyte = fragment.offset + fragment.size - 1
                    # Check if there is at least some overlap of the fragment
                    # and the parent chunk.
                    if (lastbyte >= parentoffset and
                       fragment.offset <= parentendoffset):
                        # Determine the reduced parent chunk that actually
                        # overlaps.
                        reducedparentstart = parentoffset
                        if fragment.offset > reducedparentstart:
                            reducedparentstart = fragment.offset
                        reducedparentend = parentendoffset
                        if lastbyte < reducedparentend:
                            reducedparentend = lastbyte
                        reducedparentsize = (reducedparentend + 1 -
                                             reducedparentstart)
                        # Determine the in-child offset of the reduced parent
                        # chunk.
                        inchildoffset = (childoffset + reducedparentstart -
                                         fragment.offset)
                        inparentoffset = reducedparentstart - parentoffset
                        # Have our opportunistic state object process the
                        # overlapping parent chunk at the proper offset.
                        if writemode:
                            self.ohash.written_chunk(
                              data=data[inparentoffset:inparentoffset +
                                        reducedparentsize],
                              offset=inchildoffset)
                        else:
                            self.ohash.read_chunk(
                                data=data[inparentoffset:inparentoffset +
                                          reducedparentsize],
                                offset=inchildoffset)
                        working = True
                        updated = True
                    else:
                        working = False
                else:
                    if working:
                        # Only process sparse sections if we are in the
                        # process of updating the opportunistic hash already.
                        self.ohash.sparse(length=fragment.size,
                                          offset=childoffset)
                childoffset += fragment.size
        if updated:
            # Update our range of interest.
            self.roi = self.ent.getroi(from_offset=self.ohash.offset)
            if self.ohash.isdone and (not wasdone):
                # If the data resulted in completion of opportunustic hash,
                # than log the new hash.
                self.log.write(str(self.ent) + ":" + self.ohash.result + "\n")

    # Process a written chunk
    def written_parent_chunk(self, data, parentoffset):
        self.process_parent_chunk(data=data, parentoffset=parentoffset,
                                  writemode=True)

    # Process a read chunk
    def read_parent_chunk(self, data, parentoffset):
        self.process_parent_chunk(data=data, parentoffset=parentoffset,
                                  writemode=False)

    # Fetch the offset of the first un-hashed data
    def hashing_offset(self):
        return self.ohash.offset

    # Get the opportunistic hasing result.
    def hashing_result(self):
        return self.ohash.result

    # Check if the opportunistic hashing has completed.
    def hashing_isdone(self):
        return self.ohash.isdone

    # Indicate that the mutable entity is now frozen and no more writes will
    # occur.
    def freeze(self):
        wasdone = self.ohash.isdone
        self.ohash.freeze()
        if self.ohash.isdone and (wasdone is False):
            # If freezing resulted in the hashing being done: log the result.
            self.log.write(str(self.ent) + ":" + self.ohash.result + "\n")


# Collection of repository CarvPath's still active in MattockFS and possible
# candidates for opportunistic hashing.
class OpportunisticHashCollection:
    def __init__(self, carvpathcontext, ohash_log):
        self.context = carvpathcontext
        self.ohash = dict()  # Start off with an empty collection of
        #                      opportunistic hashing candidates,
        self.log = open(ohash_log, "a", 0)  # Open a log file to keep track of
        #                                     successfull opportunistic hashing

    # Add a new carvpath to the collection.
    def add_carvpath(self, carvpath):
        # Parse the carvpath.
        ent = self.context.parse(path=carvpath)
        # Create a new opportunistic hashing candidate.
        self.ohash[carvpath] = _OH_Entity(entity=ent, log=self.log)

    # Drop a candidate from the collection.
    def remove_carvpath(self, carvpath):
        del self.ohash[carvpath]

    # Process data written to the underlying data archive.
    def lowlevel_written_data(self, offset, data):
        for carvpath in self.ohash.keys():
            self.ohash[carvpath].written_parent_chunk(data=data,
                                                      parentoffset=offset)

    # Process data read from the underlying data archive.
    def lowlevel_read_data(self, offset, data):
        for carvpath in self.ohash.keys():
            self.ohash[carvpath].read_parent_chunk(data=data,
                                                   parentoffset=offset)

    # Query if hashing for a given CarvPath has fully completed.
    def hashing_isdone(self, carvpath):
        return self.ohash[carvpath].hashing_isdone()

    # Get the hashing result for a carvpath
    def hashing_value(self, carvpath):
        return self.ohash[carvpath].hashing_result()

    # Get the offset within the CarvPath of data that has not yet been
    # opportunistically hashed.
    def hashing_offset(self, carvpath):
        return self.ohash[carvpath].hashing_offset()

    # Indicate that a mutable entity has just been frozen and no more writes
    # shall occur.
    def freeze(self, carvpath):
        self.ohash[carvpath].freeze()

if __name__ == "__main__":  # pragma: no cover
    import carvpath
    context = carvpath.Context({}, 160)
    ohc = OpportunisticHashCollection(context, "./test.log")

    ohc.add_carvpath("10+5")  # 10,11,12,13,14
    ohc.add_carvpath("13+5")  # 13,14,15,16,17
    print
    print "Bad stuff"
    ohc.lowlevel_written_data(0, b"Bad stuff")  # 0..8
    print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
    print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5")
    print
    print "Good stuff part 1 (Good)"
    ohc.lowlevel_read_data(9, b"Good")  # 9 .. 12
    print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
    print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5")
    print
    print "Spoil a bit"
    ohc.lowlevel_written_data(10, b"o")
    print
    print "Good stuff part 2 (d stuff)"
    ohc.lowlevel_read_data(12, b"d stuff")  # 12 .. 18
    print ohc.hashing_isdone("10+5"), ohc.hashing_isdone("13+5")
    print ohc.hashing_offset("10+5"), ohc.hashing_offset("13+5")
    print ohc.hashing_value("10+5"), ohc.hashing_value("13+5")
