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
import time


# Default implementation of < for argument list.
def _defaultlt(al1, al2):
    for index in range(0, len(al1)):
        if al1[index] < al2[index]:
            return True
        if al1[index] > al2[index]:
            return False
    return False


# Sortable carvpath/arglist object that allows for custom sort to be used.
class _CustomSortable:
    def __init__(self, carvpath, ltfunction, arglist):
        self.carvpath = carvpath
        self.ltfunction = ltfunction
        self.arglist = []
        # Copy the values from the arglist that apply to the carvpath
        for somemap in arglist:
            if carvpath in somemap:
                self.arglist.append(somemap[carvpath])

    # Implemention of < that should make an array of these objects sortable.
    def __lt__(self, other):
        return self.ltfunction(al1=self.arglist, al2=other.arglist)


# The reference counting stack object.
class CarvpathRefcountStack:
    def __init__(self, carvpathcontext, fadvise, ohashcollection,
                 refcount_log):
        self.context = carvpathcontext
        self.fadvise = fadvise
        self.ohashcollection = ohashcollection
        self.content = dict()  # Dictionary with all entities in the box.
        # Entity refcount for handling multiple instances of the exact same
        # entity.
        self.entityrefcount = dict()
        # A stack of fragments with different refcounts for keeping reference
        # counts on fragments.
        self.fragmentrefstack = []
        # At least one empty entity on the stack
        self.fragmentrefstack.append(self.context.empty())
        self.log = open(refcount_log, "a", 0)

    # Extract fadvise state infro on a single carvpath.
    def carvpath_fadvise_info(self, carvpath):
        ent = self.context.parse(path=carvpath)
        if (
          ent.totalsize == 0 or
          len(self.fragmentrefstack) == 0 or
          self.fragmentrefstack[0].totalsize == 0):
            overlapsize = 0
        else:
            # Create a copy of the entity without any sparse in it.
            nonsparse = ent.copy(stripsparse=True)
            # Calculate the overlap size between ent and refcount>0 data.
            overlapsize = nonsparse.overlap_size(
                            entity=self.fragmentrefstack[0])
        return [overlapsize, ent.totalsize - overlapsize]

    # Serialize whole stack; for debug purposes only.
    def __str__(self):  # pragma: no cover
        rval = ""
        for index in range(0, len(self.fragmentrefstack)):
            rval += ("   + L" +
                     str(index) +
                     " : " +
                     str(self.fragmentrefstack[index]) +
                     "\n")
        for carvpath in self.content:
            rval += ("   * " +
                     carvpath +
                     " : " +
                     str(self.entityrefcount[carvpath]) +
                     "\n")
        return rval

    # def __hash__(self):
    #    return hash(str(self))

    # Add a new entity to the stack. Returns two entities:
    # 1) An entity with all fragments that went from zero to one reference
    #    count.(can be used for fadvise purposes)
    # 2) An entity with all fragments already in the box before add was
    #    invoked (can be used for opportunistic hashing purposes).
    def add_carvpath(self, carvpath):
        if carvpath in self.entityrefcount.keys():
            # Each CarvPath exists on the stack only once.
            self.entityrefcount[carvpath] += 1
            ent = self.content[carvpath]
            # Nothing was added.
            return [self.context.empty(), ent]
        else:
            # A new carvpath entity needs a new opportunistic
            # hashing state.
            self.ohashcollection.add_carvpath(carvpath=carvpath)
            # Create a sparse free entity from carvpath.
            ent = self.context.parse(path=carvpath)
            ent.stripsparse()
            self.content[carvpath] = ent
            # Start refcount at one for this carvpath
            self.entityrefcount[carvpath] = 1
            # Extend the stack with the non-sparse data from this carvpath.
            r = self._stackextend(level=0, entity=ent)
            merged = r[0]
            # Update the fadvise value for all refcount=0 -> refcount=1
            # transitions.
            for fragment in merged:
                self.fadvise(offset=fragment.offset, size=fragment.size,
                             willneed=True)
            # Write to our refcount log file.
            self.log.write(str(time.time()) + ":+:" + str(merged)+"\n")
        return

    # Remove an existing entity from the box. Returns two entities:
    # 1) An entity with all fragments that went from one to zero refcount
    #    (can be used for fadvise purposes).
    # 2) An entity with all fragments still remaining in the box.
    def remove_carvpath(self, carvpath):
        if carvpath not in self.entityrefcount.keys():
            raise IndexError("Carvpath " + carvpath +
                             " not found on refcount stack.")
        self.entityrefcount[carvpath] -= 1
        if self.entityrefcount[carvpath] == 0:
            # Reference count has reached zero, remove from content/refcount
            ent = self.content.pop(carvpath)
            del self.entityrefcount[carvpath]
            # Remove carvpath as opportunistic hasing candidate
            self.ohashcollection.remove_carvpath(carvpath=carvpath)
            # Deminish the stack with the non-sparse parts of this carvpath.
            r = self._stackdiminish(
                        level=len(self.fragmentrefstack)-1,
                        entity=ent)
            unmerged = r
            if unmerged is not None:
                # If something has gone from refcount>0 to refcount=0,
                # then update fadvise
                for fragment in unmerged:
                    self.fadvise(offset=fragment.offset, size=fragment.size,
                                 willneed=False)
                self.log.write(str(time.time()) + ":-:" + str(unmerged)+"\n")
        return

    def _create_sortmap_R(self, startset):
        Rmap = {}
        # Higest refcount level first down to the refcount=1 level
        # untill we find some overlap.
        stacksize = len(self.fragmentrefstack)
        looklevel = stacksize - 1
        somethingfound = False
        for index in range(looklevel, 0, -1):
            hrentity = self.fragmentrefstack[index]
            # Search all overlaps at this level that are part of
            # the input set.
            for carvpath in startset:
                if hrentity.overlaps(self.content[carvpath]):
                    Rmap[carvpath] = True
                    somethingfound = True
                else:
                    Rmap[carvpath] = False
            # If we have at least one match, break from for loop.
            if somethingfound:
                break
        if stacksize == 0:
            for carvpath in startset:
                Rmap[carvpath]=False  
        return Rmap

    def _create_sortmap_r(self, startset):
        rmap = {}
        stacksize = len(self.fragmentrefstack)
        if stacksize > 0:
            # Only interested in refcount=1
            hrentity = self.fragmentrefstack[0]
            for carvpath in startset:
                if hrentity.overlaps(self.content[carvpath]):
                    rmap[carvpath] = True
                else:
                    rmap[carvpath] = False
        else:
            for carvpath in startset:
                rmap[catvpath] = False
        return rmap

    def _create_sortmap_O(self, startset):
        omap = {}
        for carvpath in startset:
            offset = None
            # Find fragment with the lowest offset.
            for frag in self.content[carvpath].fragments:
                if (offset is None or frag.issparse is False and frag.offset < offset):
                    offset = frag.offset
            omap[carvpath] = offset
        return omap

    def _create_sortmap_D(self, startset):
        Dmap = {}
        stacksize = len(self.fragmentrefstack)
        looklevel = stacksize - 1
        for index in range(looklevel, 0, -1):
            hrentity = self.fragmentrefstack[index]
            hasmatch = False
            for carvpath in startset:
                if hrentity.overlaps(
                  self.content[carvpath]):
                    # If overlaps: get+store density
                    Dmap[carvpath] = (
                        self.content[carvpath].density(
                            entity=hrentity))
                    hasmatch = True
                else:
                    Dmap[carvpath] = 0.0
            if hasmatch:
                break
        return Dmap

    def _create_sortmap_S(self, startset):
        smap = {}
        for carvpath in startset:
            if carvpath in self.content:
                smap[carvpath] = (self.content[carvpath].totalsize)
        return smap

    def _create_sortmap_W(self, startset):
        wmap = {}
        for carvpath in startset:
            accumdensity = 0
            for index in range(
                   0,
                   len(self.fragmentrefstack)):
                accumdensity += (
                  self.content[carvpath].density(
                    entity=self.fragmentrefstack[
                             index]))
            wmap[carvpath] = accumdensity
        return wmap

    def _create_sortmap_d(self, startset):
        dmap = {}
        stacksize = len(self.fragmentrefstack)
        if stacksize > 0:
            for carvpath in startset:
                l=self.fragmentrefstack[0]
                for carvpath in startset:
                    if l.overlaps(
                      self.content[carvpath]):
                        dmap[carvpath] = (
                            self.content[carvpath].density(
                              entity=l))
                    else:
                        dmap[carvpath] = 0.0
        return dmap

    def _create_sortmap_H(self, startset):
        hmap = {}
        for carvpath in startset:
            offset = None
            hmap[carvpath] = self.ohashcollection.hashing_offset(carvpath)
        return hmap

    # Pick the best job after custom sorting.
    def priority_custompick(self, params, ltfunction=_defaultlt,
                             intransit=None, reverse=False):
        # Set of maps to hand to use in custom sortable creation.
        hmap = {}  # H(ashing offset)
        # List of arguments for sorting, initially empty
        arglist = []
        # Use intransit if its given, use all jobs if not.
        startset = intransit
        if startset is None:
            startset = set(self.content.keys())
        stacksize = len(self.fragmentrefstack)
        # Process the seperate letters of the job selection policy string
        for letter in params:
            if letter == "R":
                arglist.append(self._create_sortmap_R(startset=startset))
            else:
                if letter == "r":
                    arglist.append(self._create_sortmap_r(startset=startset))
                else:
                    if letter == "O":  # Offset
                        arglist.append(self._create_sortmap_O(startset=startset)) 
                    else:
                        if letter == "D":  # Density
                            arglist.append(self._create_sortmap_O(startset=startset))
                        else:
                            if letter == "S":  # Size
                                arglist.append(self._create_sortmap_S(startset=startset))
                            else:
                                if letter == "W":  # Weighted average refcount
                                    arglist.append(self._create_sortmap_W(startset=startset))
                                else:
                                    if letter == "d": #Density
                                        arglist.append(self._create_sortmap_d(startset=startset))
                                    else:
                                        if letter == "H":
                                            arglist.append(self._create_sortmap_H(startset=startset))
                                        else:
                                            raise RuntimeError(
                                              "Invalid letter '" +
                                              letter +
                                              "' for pickspecial policy")
        # Create a new array with CustomSortable objects.
        sortable = []
        for carvpath in startset:
            sortable.append(_CustomSortable(carvpath, ltfunction, arglist))
        # Sort according to the pollicies.
        sortable.sort(reverse=reverse)
        return sortable[0]

    # Extend the stack with fragments from entity.
    # Recursive function starting at level zero.
    def _stackextend(self, level, entity):
        # If level does not exist, create it as empty.
        if not (level < len(self.fragmentrefstack)):
            self.fragmentrefstack.append(self.context.empty())
        ent = self.fragmentrefstack[level]
        # Merge with current level.
        res = ent.merge(entity)
        merged = res[1]
        unmerged = res[0]
        # Recursively call self for next level with unmerged frags
        if (len(unmerged.fragments) != 0):
            self._stackextend(level + 1, unmerged)
        return [merged, unmerged]

    # Deminish the stack with fragments from entity.
    # Recursive function starting at highest level first.
    def _stackdiminish(self, level, entity):
        # Start with unmerging at this level
        ent = self.fragmentrefstack[level]
        res = ent.unmerge(entity)
        unmerged = res[1]
        remaining = res[0]
        # If unmerging resulted in depletion of this level, remove level from
        # stack.
        if len(self.fragmentrefstack[level].fragments) == 0:
            self.fragmentrefstack.pop(level)
        # If there are additional fragments to unmerge, look at processing
        # these
        if len(remaining.fragments) > 0:
            # Should not happen at level zero.
            if level == 0:
                raise RuntimeError(
                  "Data remaining after _stackdiminish at level 0")
            # Unmerge at one level lower and anything below that.
            return self._stackdiminish(level - 1, remaining)
        else:
            if level == 0:
                # At level zero, return the refcount1->refcount0 fragments.
                return unmerged
            else:
                return None


if __name__ == "__main__":  # pragma: no cover
    class FakeFadviseFunctor:
        def __call__(self, offset, size, willneed):
            if willneed:
                print "FakeFadviseFunctor: ", offset, size, "NORMAL"
            else:
                print "FakeFadviseFunctor: ", offset, size, "DONTNEED"
    import carvpath
    import opportunistic_hash
    fadvise = FakeFadviseFunctor()
    context = carvpath.Context({}, 160)
    col = opportunistic_hash.OpportunisticHashCollection(context, "./test.log")
    stack = CarvpathRefcountStack(context, fadvise, col, "./test2.log")
    stack.add_carvpath("181481349+1234567")
    print str(stack)
    stack.add_carvpath("183950483+1234567")
    print str(stack)
    stack.add_carvpath("182715916+1234567")
    print str(stack)
    stack.add_carvpath(
      "123+1000_S9000_234+1000_S9000_345+9000_S99000_456+9000_S999000_567+9000"
      "_678+9000_S1000000_789+9000_S2000000_1234+8000_S3000000_2345+8000_"
      "S4000000_3456+8000_S5000000_4567+8000_S6000000_5678+8000_S7000000_"
      "6789+8000_S8000000")
    print str(stack)
    stack.remove_carvpath(
      "123+1000_S9000_234+1000_S9000_345+9000_S99000_456+9000_S999000_567+9000"
      "_678+9000_S1000000_789+9000_S2000000_1234+8000_S3000000_2345+8000_"
      "S4000000_3456+8000_S5000000_4567+8000_S6000000_5678+8000_S7000000_"
      "6789+8000_S8000000")
    print str(stack)
    stack.remove_carvpath("181481349+1234567")
    print str(stack)
    stack.remove_carvpath("183950483+1234567")
    print str(stack)
    stack.remove_carvpath("182715916+1234567")
    print str(stack)
