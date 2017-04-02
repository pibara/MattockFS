#!/usr/bin/python
#This is both a test and a demo of the EventLoop usage in the base mattock
#language binding. 
from mattock.api import EventLoop
import pytsk3

class Partition:
    def __init__(self,carvpath,ptype):
        self.carvpath=carvpath
        self.ptype=ptype
        if ptype[-2:].lower() == "fs":
            self.mime = "application/file-system"
        else:
            if ptype.lower() == "unallocated":
                self.mime = "disk-partition/unallocated"
            else:
                self.mime = "disk-partition/other"
    def get_carvpath(self,allocate_storage):
        print "child carvpath:",self.carvpath
        return self.carvpath
    def children(self):
        return []
    def get_meta(self):
        meta = {}
        meta["partition-type"] = self.ptype
        meta["mime-type"] = self.mime
        print "child meta:",meta
        return meta

    
class RootNode:
    def __init__(self,carvpathfile):
        self.cp=carvpathfile
        imgpath = carvpathfile.as_file_path()
        print imgpath
        img=pytsk3.Img_Info(imgpath)
        self.volume = pytsk3.Volume_Info(img)
    def children(self):
        blocksize = self.volume.info.block_size
        for part in self.volume:
            poffset = part.start * blocksize
            psize = part.len * blocksize
            pdesc = part.desc
            carvpath = self.cp[str(poffset) + "+" + str(psize)]
            yield Partition(carvpath.as_file_path(),pdesc)
    def get_meta(self):
        meta = {}
        meta["block_size"] = self.volume.info.block_size
        meta["endian"] = str(self.volume.info.endian)
        meta["part_count"] = self.volume.info.part_count
        meta["vstype"] = str(self.volume.info.vstype)
        print "top meta: ", meta
        return meta

class MmlsModule:
    def root(self,carvpathfile,arg):
        return RootNode(carvpathfile)

mmls = MmlsModule()
el = EventLoop("mmls",mmls)
el()
