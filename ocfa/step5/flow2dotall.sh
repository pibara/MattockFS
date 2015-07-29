#!/bin/bash
#Copyright 2015 Rob J meijer / University College Dublin
#This software may be used, changed and distributed under the terms of the Boost Software License 1.0
#   http://opensource.org/licenses/BSL-1.0 
#
#Create dot files using flow2dot.py script and convert to eps for use in OCFA appendix.
./flow2dot.py stripped1 > stripped1_modules.dot
fdp -Tps stripped1_modules.dot -o stripped1_modules.eps
./flow2dot.py stripped2 > stripped2_modules.dot
fdp -Tps stripped2_modules.dot -o stripped2_modules.eps
./flow2dot.py stripped3 > stripped3_modules.dot
fdp -Tps stripped3_modules.dot -o stripped3_modules.eps
./flow2dot.py stripped4 > stripped4_modules.dot
fdp -Tps stripped4_modules.dot -o stripped4_modules.eps


