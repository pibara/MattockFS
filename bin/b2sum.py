#!/usr/bin/python
from sys import argv
from pyblake2 import blake2b
h=blake2b(digest_size=32)
f = open(argv[1],"rb");
while True:
    piece = f.read(65536)  
    if not piece:
        break
    h.update(piece)
f.close()
d=h.hexdigest();
print d,argv[1]
