#!/usr/bin/python
#Copyright (c) 2015, Rob J Meijer.
#Copyright (c) 2015, University College Dublin
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#1. Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
#2. Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#3. All advertising materials mentioning features or use of this software
#   must display the following acknowledgement:
#   This product includes software developed by the <organization>.
#4. Neither the name of the <organization> nor the
#   names of its contributors may be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
#EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
#
#This file is a place holder for a future provenance loging facility.
#
import time
import json
class ProvenanceLog:
  def __init__(self,jobid,module,router_state,carvpath,mimetype,extension,parentcp=None,parentjob=None,journal=None,provenance_log=None,restore=False):
    self.log=[]
    self.journal=journal
    self.provenance=provenance_log
    rec={"job" : jobid,"module" : module,"router_state": router_state, "carvpath" : carvpath,"mime" : mimetype,"extension": extension}
    rec["time"]=time.time()
    if parentcp != None:
      rec["parent_carvpath"] = parentcp
    if parentjob!=None:
      rec["parent_job"]=parentjob
    self.log.append(rec)
    key=carvpath + "-" + jobid
    journal_rec= {"type" : "NEW", "key" : key, "provenance" : rec}
    if restore == False:
      self.journal.write(json.dumps(journal_rec))
      self.journal.write("\n")
  def __del__(self):
    rec={}
    rec["time"]=time.time()
    self.log.append(rec)
    key=self.log[0]["carvpath"] + "-" + self.log[0]["job"]
    journal_rec= {"type" : "FNL", "key" : key}
    self.journal.write(json.dumps(journal_rec))
    self.journal.write("\n")
    self.provenance.write(json.dumps(self.log))
    self.provenance.write("\n")
  def __call__(self,jobid,module,router_state="",restore=False):
    self.log.append({"jobid": jobid,"module" : module,"router_state": router_state})
    key=self.log[0]["carvpath"] + "-" + self.log[0]["job"]
    if restore == False:
      journal_rec = {"type" : "UPD", "key" : key, "provenance" : {"jobid": jobid,"module" : module,"router_state":router_state}}
      self.journal.write(json.dumps(journal_rec))
      self.journal.write("\n")
