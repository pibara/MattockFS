#!/usr/bin/python
# NOTE: This script is meant mainly for testing purposes.
#       It does not respect mattock throttling considerations
import pyewf
import sys

ewffiles = sys.argv[1:]
if len(ewffiles) == 0:
    print "Please specify EWF file to use."
else:
    handle = pyewf.handle()
    handle.open(ewffiles)
    remaining = handle.get_media_size()
    with open("./outout.dd", "w") as f:
        while remaining > 0:
            if remaining > 1048576:
                chunk = 1048576
                remaining -= chunk
            else:
                chunk = remaining
                remaining = 0
            data = handle.read(chunk)
            f.write(data)
    handle.close()
