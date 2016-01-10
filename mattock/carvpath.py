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
import os
import fcntl


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


# A fragent represents a contiguous section of a higher level data entity.
class Fragment:
    # Constructor can either be called with a fragment carvpath token string
    # or with an offset and size.
    # A fragment carvpath token string is formatted like '<offset>+<size>',
    # for example : '2048+512'
    def __init__(self, offset=None, size=None, fragpath=None):
        if fragpath is None:
            if offset is None or size is None:
                raise RuntimeError(
                       "No valid arguments for Fragment constructor")
            self.offset = offset
            self.size = size
        else:
            (self.offset, self.size) = map(int, fragpath.split('+'))

    def copy(self):
        if self.size > 0:
            return Fragment(offset=self.offset, size=self.size)
        return Sparse(size=0)

    # Casting Fragment to a carvpath string
    def __str__(self):
        if self.size == 0:
            return "S0"
        return str(self.offset) + "+" + str(self.size)

    def __hash__(self):
        return hash(str(self))

    def __lt__(self, other):
        if other is None:
            return False
        if self.offset != other.offset:
            return self.offset < other.offset
        return self.size < other.size

    def __gt__(self, other):
        return other < self

    def __eq__(self, other):
        if other is None:
            return False
        return self.offset == other.offset and self.size == other.size

    def __ne__(self, other):
        if other is None:
            return True
        return self.offset != other.offset or self.size != other.size

    def __le__(self, other):
        return not (self > other)

    def __ge__(self, other):
        return not (self < other)

    def getoffset(self):
        return self.offset

    # This is how we distinguis a fragment from a sparse description
    def issparse(self):
        # A zero size fragment will act as a sparse description!
        return self.size == 0

    # If needed we can grow a fragment; only do this with the last fragment
    # in an entity
    def grow(self, sz):
        self.size += sz


# A Sparse object represents a higher level sparse definition that can be
# thought of as
# empty space that has no immage on any lower level data.
class Sparse:
    # Constructor can either be called with a sparse carvpath token string or
    # with a size.
    # A sparse carvpath token string has the form 'S<size>',
    # for example: 'S8192'
    def __init__(self, size=None, sparsepath=None):
        if size is None:
            if sparsepath is None:
                raise RuntimeError(
                        "No valid arguments for Fragment constructor")
            self.size = int(sparsepath[1:])
        else:
            self.size = size

    def copy(self):
        return Sparse(size=self.size)

    # Casting to a carvpath string
    def __str__(self):
        return "S" + str(self.size)

    def __hash__(self):
        return hash(str(self))

    def __lt__(self, other):
        return self.size < other.size

    def __gt__(self, other):
        return self.size > other.size

    def __eq__(self, other):
        return self.size == other.size

    def __ne__(self, other):
        return self.size != other.size

    def __le__(self, other):
        return self.size <= other.size

    def __ge__(self, other):
        return not (self.size >= other.size)

    # Calling this method on a Sparse will throw an runtime exception.
    def getoffset(self):
        raise RuntimeError("Sparse doesn't have an offset")

    # This is how we distinguis a fragment from a sparse description
    def issparse(self):
        return True

    # If needed we can grow a sparse region; only do this with the last
    # fragment in an entity
    def grow(self, sz):
        self.size += sz


# Helper function for creating either a sparse region or a fragment from a
# carvpath fragment/sparse token.
def _asfrag(fragstring):
    if fragstring[0] == 'S':
        return Sparse(sparsepath=fragstring)
    else:
        rval = Fragment(fragpath=fragstring)
        # If we have a zero size fragment, represent as
        # zero size sparse instead.
        if rval.size == 0:
            return Sparse(size=0)
        return rval


# An entity is an ordered  collection of Fragment and/or Sparse objects.
# Entities are the core concept within pycarvpath.
class _Entity:
    # An Entity constructor takes either a carvpath, a list of pre-made
    # fragments or no constructor argument at all if you wish to create a new
    # empty Entity. You should probably not be instantiating your own _Entity
    # objects.
    # Instead, create entities using the 'parse' method of a Contect object.
    # An _Entity carvpath consists of one or more Fragment and/or Sparse
    # carvpath tokens seperated by a '_' character.for example:
    #  '0+4096_S8192_4096+4096'
    def __init__(self, lpmap, maxfstoken, carvpath=None, fragments=None):
        self.longpathmap = lpmap
        self.maxfstoken = maxfstoken
        if fragments is None:
            fragments = []
        self.fragments = []
        if carvpath is not None:
            # Any carvpath starting with a capital D must be looked up in our
            # longtoken database
            if carvpath[0] == 'D':
                carvpath = self.longpathmap[carvpath]
            # Apply  _asfrag helper function to each fragment in the carvpath
            # and use it to initialize our fragments list
            fragments = map(_asfrag, carvpath.split("_"))
        self.totalsize = 0
        # Add the fragments one at a time.
        for frag in fragments:
            self.unaryplus(other=frag)

    # Copy with or without sparse sections.
    def copy(self, stripsparse=False):
        if stripsparse:
            fragments = []
            for frag in self.fragments:
                if frag.issparse() == False:
                    fragments.append(frag.copy())
            return _Entity(lpmap=self.longpathmap,
                           maxfstoken=self.maxfstoken,
                           fragments=fragments)
        else:
            return _Entity(
                    lpmap=self.longpathmap,
                    maxfstoken=self.maxfstoken,
                    fragments=copy.deepcopy(self.fragments))

    # Use a secure hash function to get a shorter representation of carvpath.
    # Than register the shorter representation  in redis so we can find the
    # original carvpath back from other processes.
    def _asdigest(self, path):
        rval = "D" + blake2b(path.encode(), digest_size=32).hexdigest()
        self.longpathmap[rval] = path
        return rval

    # If desired, invoke the _Entity as a function to get the fragments it is
    # composed of.
    def __cal__(self):
        for index in range(0, len(self.fragments)):
            yield self.fragments[index]

    # You may use square brackets to acces specific fragments.
    def __getitem__(self, index):
        return self.fragments[index]

    # Grow the entity by extending on its final fragment or, if there are non,
    # by creating a first fragment with offset zero.
    def grow(self, chunksize):
        if len(self.fragments) == 0:
            self.fragments.append(Fragment(offset=0, size=chunksize))
        else:
            self.fragments[-1].grow(sz=chunksize)
        self.totalsize += chunksize

    # Casting to a carvpath string
    def __str__(self):
        # Anything of zero size is represented as zero size sparse region.
        if len(self.fragments) == 0:
            return "S0"
        # Apply a cast to string on each of the fragment and concattenate the
        # result using '_' as join character.
        rval = "_".join(map(str, self.fragments))
        # If needed, store long carvpath in database and replace the long
        # carvpath with its digest.
        if len(rval) > self.maxfstoken:
            return self._asdigest(rval)
        else:
            return rval

    def __hash__(self):
        return hash(str(self))

    # Implementation of < for Entity objects
    def __lt__(self, other):
        # Empty entities have the lowest value
        if self.totalsize == 0 and other.totalsize > 0:
            return True
        if other.totalsize == 0:
            return False
        # Compare at most the number of fragments determined by the
        # entity with the fewest fragments.
        ownfragcount = len(self.fragments)
        otherfragcount = len(other.fragments)
        sharedfragcount = ownfragcount
        if otherfragcount < sharedfragcount:
            sharedfragcount = otherfragcount
        for index in range(0, sharedfragcount):
            # Sparse entries have a lower value than fragments.
            if (self.fragments[index].issparse() !=
               other.fragments[index].issparse()):
                return self.fragments[index].issparse()
            if (not self.issparse()):
                # Lower fragment offset means lower sort value.
                if (self.fragments[index].offset !=
                   other.fragments[index].offset):
                    return (self.fragments[index].offset <
                            other.fragments[index].offset)
            # Lower fragment/sparse size means lower sort value.
            if self.fragments[index].size != other.fragments[index].size:
                return (self.fragments[index].size <
                        other.fragments[index].size)
        # If eny of the entities has less fragments, the sort value is lower
        return (ownfragcount < otherfragcount)

    def __gt__(self, other):
        return other < self

    def __le__(self, other):
        return not (self > other)

    def __ge__(self, other):
        return not (self < other)

    def __eq__(self, other):
        if not isinstance(other, _Entity):
            return False
        # Total sizes need to be the same
        if self.totalsize != other.totalsize:
            return False
        # Number of fragments need to be the same
        if len(self.fragments) != len(other.fragments):
            return False
        # And every fragment needs to be the same as that fragment in the
        # other entity.
        for index in range(0, len(self.fragments)):
            # Either both need to be fragments or both have to be sparse.
            if (self.fragments[index].issparse() !=
                    other.fragments[index].issparse()):
                return False
            # For fragments the offset needs to be the same.
            if ((not self.fragments[index].issparse()) and
               self.fragments[index].offset != other.fragments[index].offset):
                return False
            # Finaly the size of fragment/sparse must be the same.
            if self.fragments[index].size != other.fragments[index].size:
                return False
        return True

    def __ne__(self, other):
        return not self == other

    # Python does not allow overloading of any operator+= ; this method
    # pretends it does. Implements : ent += other.
    def unaryplus(self, other):
        if isinstance(other, _Entity):
            # We can either append a whole Entity
            for index in range(0, len(other.fragments)):
                self.unaryplus(other=other.fragments[index])
        else:
            # Or a single fragment.
            # If the new fragment is directly adjacent and of the same type,
            # we don't add it but instead we grow the last existing fragment.
            if (len(self.fragments) > 0 and
               self.fragments[-1].issparse() == other.issparse() and
               (other.issparse() or
               (self.fragments[-1].getoffset() +
                self.fragments[-1].size) ==
               other.getoffset())):
                self.fragments[-1] = self.fragments[-1].copy()
                self.fragments[-1].grow(sz=other.size)
            else:
                # Otherwise we append the new fragment.
                self.fragments.append(other)
            # Meanwhile we adjust the totalsize member for the entity.
            self.totalsize += other.size

    # Appending two entities together, merging the tails if possible.
    def __add__(self, other):
        # Express a+b in terms of operator+=
        rval = _Entity(lpmap=self.longpathmap, maxfstoken=self.maxfstoken)
        rval.unaryplus(other=this)
        rval.unaryplus(other=other)
        return rval

    # Helper generator function for getting the per-fragment chunks for a
    # subentity.
    # The function yields the parent chunks that fit within the offset/size
    # indicated relative to the parent entity.
    def _subchunk(self, offset, size, truncate):
        # We can't find chunks beyond the parent total size
        if (offset+size) > self.totalsize:
            if truncate:
                if offset >= self.totalsize:
                    size = 0
                else:
                    size = self.totalsize - offset
            else:
                raise IndexError('Not within parent range')
        # We start of at offset 0 of the parent entity whth our initial offset
        # and size.
        start = 0
        startoffset = offset
        startsize = size
        # Process each parent fragment
        for parentfrag in self.fragments:
            # Skip the fragments that fully exist before the ofset/size region
            # we are looking for.
            if (start + parentfrag.size) > startoffset:
                # Determine the size of the chunk we need to process
                maxchunk = parentfrag.size + start - startoffset
                if maxchunk > startsize:
                    chunksize = startsize
                else:
                    chunksize = maxchunk
                # Yield the proper type of fragment
                if chunksize > 0:
                    if parentfrag.issparse():
                        yield Sparse(size=chunksize)
                    else:
                        yield Fragment(
                               offset=parentfrag.getoffset()+startoffset-start,
                               size=chunksize)
                    # Update startsize for the rest of our data
                    startsize -= chunksize
                # Update the startoffset for the rest of our data
                if startsize > 0:
                    startoffset += chunksize
                else:
                    # Once the size is null, update the offset as to skip the
                    # rest of the loops
                    startoffset = self.totalsize + 1
            start += parentfrag.size

    # Get the projection of an entity as sub entity of an other entity.
    def subentity(self, childent, truncate=False):
        subentity = _Entity(lpmap=self.longpathmap, maxfstoken=self.maxfstoken)
        for childfrag in childent.fragments:
            if childfrag.issparse():
                subentity.unaryplus(other=childfrag)
            else:
                for subfrag in self._subchunk(offset=childfrag.offset,
                                              size=childfrag.size,
                                              truncate=truncate):
                    subentity.unaryplus(other=subfrag)
        return subentity

    # Python has no operator=, so we use assigtoself
    def assigntoself(self, other):
        self.fragments = other.fragments
        self.totalsize = other.totalsize

    # Strip the entity of its sparse fragments and sort itsd non sparse
    # fragments.
    # This is meant to be used for reference counting purposes inside of
    # the Box.
    def stripsparse(self):
        newfragment = _Entity(lpmap=self.longpathmap,
                              maxfstoken=self.maxfstoken)
        nosparse = []
        for i in range(len(self.fragments)):
            if self.fragments[i].issparse() == False:
                nosparse.append(self.fragments[i])
        fragments = sorted(nosparse)
        for i in range(len(fragments)):
            newfragment.unaryplus(other=fragments[i])
        self.assigntoself(other=newfragment)

    # Merge an other sorted/striped entity and return two entities: One with
    # all fragments left unused and one with the fragments used for merging
    # into self.
    def merge(self, entity):
        rval = _merge_entities(self, entity)
        self.assigntoself(other=rval[0])
        return rval[1:]

    # Opposit of the merge function.
    def unmerge(self, entity):
        rval = _unmerge_entities(self, entity)
        self.assigntoself(other=rval[0])
        return rval[1:]

    def overlaps(self, entity):
        return _fragapply(ent1=self, ent2=entity, test=(lambda a, b: a and b))

    def overlap_size(self, entity):
        size1 = self.totalsize + entity.totalsize
        tent = entity.copy()
        tent.merge(entity=self)
        return size1 - tent.totalsize

    def density(self, entity):
        r = _fragapply(ent1=self, ent2=entity, bflist=[(lambda a, b: a and b)])
        rval = float(r[0].totalsize)/float(self.totalsize)
        return rval

    # The range of interest (roi)
    def getroi(self, from_offset):
        coffset = 0
        start = None
        end = None
        for fragment in self.fragments:
            if ((not fragment.issparse()) and
               from_offset <= coffset+fragment.size):
                if coffset <= from_offset:
                    start = fragment.offset + from_offset - coffset
                    end = fragment.offset+fragment.size - 1
                else:
                    if start > fragment.offset:
                        start = fragment.offset
                    if end < fragment.offset+fragment.size - 1:
                        end = fragment.offset+fragment.size - 1
            coffset += fragment.size
        return [start, end]


# Helper functions for mapping merge and unmerge to the higher order
# _fragapply function.
def _merge_entities(ent1, ent2):
    return _fragapply(ent1=ent1, ent2=ent2, bflist=[
              (lambda a, b: a or b),
              (lambda a, b: a and b),
              (lambda a, b: (not a) and b)])


def _unmerge_entities(ent1, ent2):
    return _fragapply(ent1=ent1, ent2=ent2, bflist=[
              (lambda a, b: a and (not b)),
              (lambda a, b: (not a) and b),
              (lambda a, b: a and b)])


# Helper function for applying boolean lambda's to each ent1/ent2 overlapping
# or non-overlapping fragment and returning an Entity with all fragments that
# resolved to true for the coresponding lambda.
def _fragapply(ent1, ent2, bflist=None, test=None):
    # If our third argument is a lambda, use it as test instead.
    if test is not None:
        testmode = True
    else:
        testmode = False
    chunks = []
    foreignfragcount = len(ent2.fragments)
    foreignfragindex = 0
    ownfragcount = len(ent1.fragments)
    ownfragindex = 0
    masteroffset = 0
    ownoffset = 0
    ownsize = 0
    ownend = 0
    foreignoffset = 0
    foreignsize = 0
    foreignend = 0
    discontinue = foreignfragcount == 0 and ownfragcount == 0
    # Walk through both entities at the same time untill we are done with all
    # fragments and have no remaining size left.
    while not discontinue:
        # Get a new fragment from the first entity if needed and possible
        if ownsize == 0 and ownfragindex != ownfragcount:
            ownoffset = ent1.fragments[ownfragindex].getoffset()
            ownsize = ent1.fragments[ownfragindex].size
            ownend = ownoffset+ownsize
            ownfragindex += 1
        # Get a new fragment from the second entity if needed and possible
        if foreignsize == 0 and foreignfragindex != foreignfragcount:
            foreignoffset = ent2.fragments[foreignfragindex].getoffset()
            foreignsize = ent2.fragments[foreignfragindex].size
            foreignend = foreignoffset+foreignsize
            foreignfragindex += 1
        # Create an array of start and end offsets and sort them
        offsets = []
        if ownsize > 0:
            offsets.append(ownoffset)
            offsets.append(ownend)
        if foreignsize > 0:
            offsets.append(foreignoffset)
            offsets.append(foreignend)
        offsets = sorted(offsets)
        # Find the part we need to look at this time around the while loop
        firstoffset = offsets[0]
        secondoffset = offsets[1]
        if secondoffset == firstoffset:
            secondoffset = offsets[2]
        # Initialize boolens
        hasone = False
        hastwo = False
        # See if this chunk overlaps with either or both of the two entities.
        if ownsize > 0 and ownoffset == firstoffset:
            hasone = True
        if foreignsize > 0 and foreignoffset == firstoffset:
            hastwo = True
        fragsize = secondoffset - firstoffset
        # If needed, insert an extra entry indicating the false/false state.
        if firstoffset > masteroffset:
            if testmode:
                if test(False, False):
                    return True
            else:
                chunks.append([masteroffset, firstoffset-masteroffset,
                               False, False])
                masteroffset = firstoffset
        if testmode:
            if test(hasone, hastwo):
                return True
        else:
            # Than append the info of our (non)overlapping fragment
            chunks.append([firstoffset, fragsize, hasone, hastwo])
        # Prepare for the next time around the loop
        masteroffset = secondoffset
        if hasone:
            ownoffset = masteroffset
            ownsize -= fragsize
        if hastwo:
            foreignoffset = masteroffset
            foreignsize -= fragsize
        # Break out of the loop as soon as everything is done.
        if (foreignfragindex == foreignfragcount and
           ownfragcount == ownfragindex and
           ownsize == 0 and foreignsize == 0):
            discontinue = True
    if testmode:
        return False
    # Create an array with Entity objects, one per lambda.
    rval = []
    for index in range(0, len(bflist)):
        rval.append(_Entity(lpmap=ent1.longpathmap,
                            maxfstoken=ent1.maxfstoken))
    # Fill each entity with fragments depending on the appropriate lambda
    # invocation result.
    for index in range(0, len(chunks)):
        off = chunks[index][0]
        size = chunks[index][1]
        oldown = chunks[index][2]
        oldforeign = chunks[index][3]
        for index2 in range(0, len(bflist)):
            if bflist[index2](oldown, oldforeign):
                rval[index2].unaryplus(other=Fragment(offset=off, size=size))
    return rval


# This object allows an Entity to be validated against an underlying data
# source with a given size.
class _Top:
    # Don instantiate a _Top, Instantiate a Context and use
    # Context::make_top instead.
    def __init__(self, lpmap, maxfstoken, size=0):
        self.size = size
        self.topentity = _Entity(lpmap=lpmap,
                                 maxfstoken=maxfstoken,
                                 fragments=[Fragment(offset=0,
                                                     size=size)])

    # Get this Top object as an Entity.
    def entity():
        return self.topentity

    # If the underlying data source changes its size by data being added,
    # grow allows you to notify the Top object of this and allow future
    # entities to exist within the extended bounds.
    def grow(self, chunk):
        self.size += chunk
        self.topentity.grow(chunksize=chunk)

    # Test if a given Entity is valid within the bounds of the Top data size.
    def test(self, child):
        try:
            b = self.topentity.subentity(childent=child)
        except IndexError:
            return False
        return True


# You need one of these per application in order to use pycarvpath.
class Context:
    # A Context needs a dict like object that implements persistent (and
    # possibly distributed) storage of long path entities and their shorter
    # representation. This pseudo dict is passed as lpmap argument.
    # By default all carvpath strings larger than 160 bytes are represented
    # by a 65 byte long token stored in this pseudo dict. You may specify a
    # different maximum carvpath lengt if you wish for a longer or shorter
    # treshold.
    def __init__(self, lpmap, maxtokenlen=160):
        self.longpathmap = lpmap
        self.maxfstoken = maxtokenlen

    # Parse a (possibly nested) carvpath and return an Entity object.
    # This method will throw if a carvpath string is invalid. It will however
    # NOT set any upper limits to valid carvpaths within a larger image.
    # If you wish to do so, create a Top object and invoke Top::test(ent) with
    # the Entity you got back from parse.
    def parse(self, path):
        levelmin = None
        for level in path.split("/"):
            level = _Entity(lpmap=self.longpathmap,
                            maxfstoken=self.maxfstoken,
                            carvpath=level)
            if levelmin is not None:
                level = levelmin.subentity(childent=level)
            levelmin = level
        return level

    # Cheate a Top object to validate parsed entities against.
    def make_top(self, size=0):
        return _Top(self.longpathmap, self.maxfstoken, size)

    def empty(self):
        return _Entity(lpmap=self.longpathmap, maxfstoken=self.maxfstoken)


class _Test:  # pragma: no cover
    def __init__(self, lpmap, maxtokenlen):
        self.context = Context(lpmap, maxtokenlen)

    def testadd(self, pin1, pin2, pout):
        a = self.context.parse(pin1)
        b = self.context.parse(pin2)
        c = self.context.parse(pout)
        a.unaryplus(other=b)
        if (a != c):
            print("FAIL: '" + pin1 + " + " + pin2 + " = " + str(a) +
                  "' expected='" + pout + "'")
        else:
            print("OK: '" + pin1 + " + " + pin2 + " = " + str(a) + "'")

    def teststripsparse(self, pin, pout):
        a = self.context.parse(pin)
        b = self.context.parse(pout)
        a.stripsparse()
        if a != b:
            print("FAIL: in='" + pin + "' expected='" + pout + "' result='" +
                  str(a) + "'")
        else:
            print("OK: in='" + pin + "' expected='" + pout + "' result='" +
                  str(a) + "'")

    def testflatten2(self, context, pin, pout):
        a = context.parse(pin)
        b = context.parse(pout)
        if a != b:
            print("FAIL: in='" + pin + "' expected='" + pout + "  (" +
                  str(b) + ") ' result='" + str(a) + "'")
        else:
            print("OK: in='" + pin + "' expected='" + pout + "' result='" +
                  str(a) + "'")

    def testflatten(self, context, pin, pout):
        a = context.parse(pin)
        if str(a) != pout:
            print("FAIL: in='" + pin + "' expected='" + pout + "' result='" +
                  str(a) + "'")
        else:
            print("OK: in='" + pin + "' expected='" + pout + "' result='" +
                  str(a) + "'")
        self.testflatten2(context, pin, pout)

    def testrange(self, topsize, carvpath, expected):
        context = Context({})
        top = context.make_top(topsize)
        entity = context.parse(carvpath)
        if top.test(entity) != expected:
            print("FAIL: topsize=" + str(topsize) + "path=" + carvpath +
                  "result=" + str(not expected))
        else:
            print("OK: topsize=" + str(topsize) + "path=" + carvpath +
                  "result=" + str(expected))

    def testsize(self, context, pin, sz):
        print("TESTSIZE:")
        a = context.parse(pin)
        if a.totalsize != sz:
            print("FAIL: in='" + pin + "' expected='" + str(sz) +
                  "' result='" + str(a.totalsize) + "'")
        else:
            print("OK: in='" + pin + "' expected='" + str(sz) + "' result='" +
                  str(a.totalsize) + "'")

    def testmerge(self, p1, p2, pout):
        print("TESTMERGE:")
        context = Context({})
        a = context.parse(p1)
        a.stripsparse()
        b = context.parse(p2)
        b.stripsparse()
        c = context.parse(pout)
        d = a.merge(entity=b)
        if a != c:
            print("FAIL : "+str(a)+"  "+str(d[0]) + " ;  "+str(d[1]))
        else:
            print("OK : "+str(a))


if __name__ == "__main__":  # pragma: no cover
    import longpathmap
    lpmap = longpathmap.LongPathMap()
    context = Context(lpmap)
    t = _Test(lpmap, 160)
    t.testflatten(context, "0+0", "S0")
    t.testflatten(context, "S0", "S0")
    t.testflatten(context, "0+0/0+0", "S0")
    t.testflatten(context, "20000+0", "S0")
    t.testflatten(context, "20000+0_89765+0", "S0")
    t.testflatten(context, "1000+0_2000+0/0+0", "S0")
    t.testflatten(context, "0+5", "0+5")
    t.testflatten(context, "S1_S1", "S2")
    t.testflatten(context, "S100_S200", "S300")
    t.testflatten(context, "0+20000_20000+20000", "0+40000")
    t.testflatten(context, "0+20000_20000+20000/0+40000", "0+40000")
    t.testflatten(context, "0+20000_20000+20000/0+30000", "0+30000")
    t.testflatten(context, "0+20000_20000+20000/10000+30000", "10000+30000")
    t.testflatten(context, "0+20000_40000+20000/10000+20000",
                           "10000+10000_40000+10000")
    t.testflatten(context, "0+20000_40000+20000/10000+20000/5000+10000",
                           "15000+5000_40000+5000")
    t.testflatten(context, "0+20000_40000+20000/10000+20000/5000+10000/"
                           "2500+5000", "17500+2500_40000+2500")
    t.testflatten(context, "0+20000_40000+20000/10000+20000/5000+10000/"
                           "2500+5000/1250+2500", "18750+1250_40000+1250")
    t.testflatten(context, "0+20000_40000+20000/10000+20000/5000+10000/"
                           "2500+5000/1250+2500/625+1250",
                           "19375+625_40000+625")
    t.testflatten(context, "0+100_101+100_202+100_303+100_404+100_505+100_"
                           "606+100_707+100_808+100_909+100_1010+100_1111+100"
                           "_1212+100_1313+100_1414+100_1515+100_1616+100_1717"
                           "+100_1818+100_1919+100_2020+100_2121+100_2222+100_"
                           "2323+100_2424+100",
                           "D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c"
                           "65335251fe3fa480")
    t.testflatten(context, "0+100_101+100_202+100_303+100_404+100_505+100_606"
                           "+100_707+100_808+100_909+100_1010+100_1111+100_"
                           "1212+100_1313+100_1414+100_1515+100_1616+100_1717"
                           "+100_1818+100_1919+100_2020+100_2121+100_2222+100"
                           "_2323+100_2424+100/1+2488",
                           "D0e2ded6b35aa15baabd679f7d8b0a7f0ad393948988b6b2f"
                           "28db7c283240e3b6")
    t.testflatten(context, "D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c"
                           "65335251fe3fa480/1+2488",
                           "D0e2ded6b35aa15baabd679f7d8b0a7f0ad393948988b6b2f"
                           "28db7c283240e3b6")
    t.testflatten(context, "D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c"
                           "65335251fe3fa480/350+100", "353+50_404+50")
    t.testflatten(context, "S200000/1000+9000", "S9000")
    t.testrange(200000000000, "0+100000000000/0+50000000", True)
    t.testrange(20000, "0+100000000000/0+50000000", False)
    t.testsize(context, "20000+0_89765+0", 0)
    t.testsize(context, "0+20000_40000+20000/10000+20000/5000+10000", 10000)
    t.testsize(context, "D901141262aa24eaaddbce2f470615b6a47639f7a62b3bc7c653"
                        "35251fe3fa480/350+100", 100)
    t.teststripsparse("0+1000_S2000_1000+2000", "0+3000")
    t.teststripsparse("1000+2000_S2000_0+1000", "0+3000")
    t.teststripsparse("0+1000_S2000_4000+2000", "0+1000_4000+2000")
    t.teststripsparse("4000+2000_S2000_0+1000", "0+1000_4000+2000")
    t.testadd("0+1000_S2000_1000+2000", "3000+1000_6000+1000",
              "0+1000_S2000_1000+3000_6000+1000")
    t.testadd("0+1000_S2000", "S1000_3000+1000", "0+1000_S3000_3000+1000")
    t.testmerge("0+1000_2000+1000", "500+2000", "0+3000")
    t.testmerge("2000+1000_5000+100",
                "100+500_800+800_4000+200_6000+100_7000+100",
                "100+500_800+800_2000+1000_4000+200_5000+100_6000+100_7000"
                "+100")
    t.testmerge("2000+1000_5000+1000", "2500+500", "2000+1000_5000+1000")
    t.testmerge("500+2000", "0+1000_2000+1000", "0+3000")
    t.testmerge("0+1000_2000+1000", "500+1000", "0+1500_2000+1000")
    t.testmerge("S0", "0+1000_2000+1000", "0+1000_2000+1000")
    t.testmerge("0+60000", "15000+30000", "0+60000")
