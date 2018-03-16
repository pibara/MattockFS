#Copyright (c) 2018, Rob J Meijer.
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
from pyblake2 import blake2b
import time
import json

class MerkleTreeLog:
    def __init__(self,logpath):
        self.starttime = time.time()
        self.maxbacklogtime = 900 #At least 15 minutes between merkletree flushes
        self.entries = list()
        self.log = self.log = open(logpath, "a", 0)
    def add(self,cp,digest):
        rec = dict()
        rec["cp"] = cp
        rec["hs"] = digest
        rec["mh"] = blake2b(cp,digest_size=32,key=digest).hexdigest()
        self.entries.append(rec)
        timepassed = time.time() - self.starttime
        if timepassed > self.maxbacklogtime and len(self.entries) > 1:
            self.flush()
    def tick(self):
        timepassed = time.time() - self.starttime
        if timepassed > self.maxbacklogtime and len(self.entries) > 1:
            self.flush()
    def flush(self):
        if len(self.entries) > 1:
            while len(self.entries) > 1:
                newentries = list()
                while len(self.entries) > 1:
                    rec = dict()
                    rec["c"] = [self.entries.pop(0),self.entries.pop(0)]
                    rec["mh"] = blake2b(rec["c"][0]["mh"],digest_size=32,key=rec["c"][1]["mh"]).hexdigest()
                    newentries.append(rec)
                if len(self.entries) == 1:
                    newentries.append(self.entries.pop(0))
                self.entries = newentries
            mtree = json.dumps(self.entries.pop(0),sort_keys=True)
            self.log.write(mtree + "\n")
            self.starttime = time.time()
            self.entries = list()



        
