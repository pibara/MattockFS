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
  def __init__(self,jobid,module,carvpath,mimetype,parentcp=None,parentjob=None,journal=None,provenance_log=None):
    self.log=[]
    self.journal=journal
    self.provenance=provenance_log
    rec={"job" : jobid,"module" : module, "carvpath" : carvpath,"mime" : mimetype}
    rec["time"]=time.time()
    if parentcp != None:
      rec["parent_carvpath"] = parentcp
    if parentjob!=None:
      rec["parent_job"]=parentjob
    self.log.append(rec)
    self.journal.write("NEW:cp="+carvpath+":firstjob="+jobid+":module="+module+":parent="+str(parentcp)+":type="+mimetype+"\n")
  def __del__(self):
    rec={}
    rec["time"]=time.time()
    self.log.append(rec)
    self.journal.write("FNL:cp="+self.log[0]["carvpath"]+":firstjob="+self.log[0]["job"]+"\n")
    self.provenance.write(json.dumps(self.log))
    self.provenance.write("\n")
  def __call__(self,jobid,module):
    self.log.append({"jobid": jobid,"module" : module})
    self.journal.write("UPD:cp="+self.log[0]["carvpath"]+":firstjob="+self.log[0]["job"]+":module="+module+"\n")

