#!/usr/bin/pytho
from subprocess import Popen, PIPE
import imghdr
import exifread
import json

def mmls(path):
    mmls_process = Popen(["/usr/bin/mmls", path], stdout=PIPE)
    (output, err) = mmls_process.communicate()
    mmls_process.wait()
    for line in output.split("\n"):
        parts = line.split()
        if len(parts) == 6 and parts[-1][-2:] == "FS":
            offset = str(int(parts[2]) * 512)
            size = str(int(parts[4]) * 512)
            yield offset, size

def istat(path,inode):
    istat_process = Popen(["/usr/bin/istat", path, inode], stdout=PIPE)
    (output, err) = istat_process.communicate()
    istat_process.wait()
    if "\nData Fork Blocks" in output:
        filesize = output[output.find("\nSize:\t")+7:].split()[0]
        rawblocks = output[output.find("\nData Fork Blocks:\n")+18:].split()[0]
        datablocks = rawblocks.split("-")
        firstblock=int(datablocks[0])
        if len(datablocks) > 2:
            return None,None,inode
        offset = firstblock * 4096
        return str(offset),filesize,inode
    else:
        return None,None,inode

def is_jpg(path):
    return imghdr.what(path) == "jpeg"

def fswalk(path,inode=None):
    if inode is None:
        fls_process = Popen(["/usr/bin/fls", path], stdout=PIPE)
    else:
        fls_process = Popen(["/usr/bin/fls", path, inode], stdout=PIPE)
    (output, err) = fls_process.communicate()
    fls_process.wait()
    for line in output.split("\n"):
        parts = line.split()
        if len(parts) > 2:
            if parts[0] == "d/d":
                for foffset,fsize,inod in fswalk(path,parts[1][:-1]):
                    yield foffset,fsize,inod
            if parts[0] == "r/r":
                foffset,fsize,inod = istat(path,parts[1][:-1])
                if not foffset is None:
                    yield foffset,fsize,inod

def exif(path):
    with open(path,"r") as f:
        exifdata=exifread.process_file(f)
        shortexif = dict()
        for key in exifdata:
            val = exifdata[key]
            if len(str(val)) < 101:
                shortexif[key]=str(exifdata[key])
        return json.dumps(shortexif,sort_keys=True,indent=4, separators=(',', ': '))
        
    

#path = "carvpath/346+103219200"
#for cp in mmls(path):
#    #print "partition",path+cp+".dd"
#    for fsfile in fswalk(path+cp):
#        print fsfilent
