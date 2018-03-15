#!/usr/bin/python
from sh import tail
import json

for line in tail("-f", "/var/mattock/log/0.merkletree", _iter=True):
    mh=json.loads(line)["mh"]
    print mh
