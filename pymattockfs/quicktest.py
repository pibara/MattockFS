#!/usr/bin/python
import xattr
mountpoint="/home/larissa/src/mattock-dissertation/pymattockfs/mnt"
modulectl=xattr.xattr(mountpoint + "/module/kickstart.ctl")
modulectl["user.reset"]="1"
instance=modulectl["user.register_instance"]
print instance
instancectl=xattr.xattr(mountpoint + "/" + instance)
instancectl["user.sort_policy"]="K"
job=instancectl["user.accept_job"]
print job
jobctl=xattr.xattr(mountpoint + "/" + job)
jobctl["user.create_mutable"] = "1234500"
newdata=jobctl["user.mutable"]
with open(mountpoint + "/" + newdata,"ab") as f:
    f.seek(0,0)
    f.write("harhar")
    f.seek(0,1234000)
    f.write("HARHAR")
print newdata
