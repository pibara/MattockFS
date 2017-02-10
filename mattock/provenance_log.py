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
# This file is a place holder for a future provenance loging facility.
#
import time
import json
from pyblake2 import blake2b


# This class keeps track of provenance info during the lifetime of a CarvPath
# toolchain within MattockFS.
class ProvenanceLog:
    def __init__(self, jobid, actor, router_state, carvpath, mimetype,
                 extension, parentcp=None, parentjob=None, journal=None,
                 provenance_log=None, restore=False, user=None, command=None):
        if parentjob == jobid:
            parentjob = None
        self.log = []  # Start with an empty provenance log.
        self.journal = journal  # This is a handle to a journal file
        self.provenance = provenance_log  # File handle for storing provenance
        #                                   log once done.
        key = carvpath + "-" + jobid
        self.key = blake2b(key.encode(), digest_size=32).hexdigest() 
        # Fill the first record in the provenance log with basic info.
        rec = {"jobid": jobid, "actor": actor, "router_state": router_state,
               "carvpath": carvpath, "mime": mimetype, "extension": extension, "active": True }
        rec["time"] = time.time()  # Log creation time
        # If there was a parent carvpath: make a note of it in the creation
        # record.
        if parentcp is not None:
            rec["parent_carvpath"] = parentcp
        # If there was a parent job: make note of it in the creation record.
        if parentjob is not None:
            rec["parent_job"] = parentjob
        if user is not None:
            rec["user"] = user
        if command is not None:
            rec["command"] = command
        # Store our first record in the provenance log array.
        self.log.append(rec)
        # Don't log to journal if constructed in restore mode.
        if restore is False:
            # Create a journal log record
            journal_rec = {"type": "NEW", "key": self.key, "provenance": rec}
            # Write the record synchonously to the journal.
            self.journal.write(json.dumps(journal_rec,sort_keys=True))
            self.journal.write("\n")
    def __del__(self):
        # When the ProvenanceLog object is deleted, create one last record.
        rec = {}
        rec["time"] = time.time()  # Make note of the time.
        rec["active"] = False
        self.log.append(rec)
        # Retreive the unique key to use in the journal.
        key = self.key
        # Create a journal record.
        journal_rec = {"type": "FNL", "key": key, "provenance" : rec}
        # Write it to the journal.
        self.journal.write(json.dumps(journal_rec,sort_keys=True))
        self.journal.write("\n")
        # Write the full provenance log to the provenance logging file.
        self.provenance.write(json.dumps(self.log,sort_keys=True))
        self.provenance.write("\n")

    def __call__(self, jobid, actor, router_state="", restore=False,command=None, user=None):
        newobj = {"jobid": jobid,
                  "actor": actor,
                  "router_state": router_state,
                  "time" : time.time(),
                  "active" : False}
        if user != None:
            newobj["user"] = user
        if command != None:
            newobj["command"] = command
        # Add a line to the provenance log.
        self.log.append(newobj)
        # Don't log to journal if invoked in  restore mode.
        if restore is False:
            # Retreive the unique key to use in the journal.
            key = self.key
            # Create a journal record
            journal_rec = {"type": "UPD", 
                           "key": key,
                           "provenance": newobj}
            # And write it to the jounal log.
            self.journal.write(json.dumps(journal_rec,sort_keys=True))
            self.journal.write("\n")
    def accept(self,actor,command,user):
        newobj = {"actor": actor,
                  "time" : time.time(),
                  "user" : user,
                  "command" : command,
                  "active" : True }
        self.log.append(newobj)
        journal_rec = {"type" : "UPD",
                       "key"  : self.key,
                       "provenance" : newobj }
        self.journal.write(json.dumps(journal_rec,sort_keys=True))
        self.journal.write("\n")
    def restorepoint(self):
        # Retreive the unique key to use in the journal.
        key = self.key
        # Create a journal record
        journal_rec = {"type": "RPENT", 
                       "key": key,
                       "provenance": self.log}
        # And write it to the jounal log.
        self.journal.write(json.dumps(journal_rec,sort_keys=True))
        self.journal.write("\n")


