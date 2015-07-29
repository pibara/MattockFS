#!/bin/bash
./eventdump.py ./stripped1 | sort -n |./virtcachesize.py stripped1_virtcachesize.eps
./eventdump.py ./stripped2 | sort -n |./virtcachesize.py stripped2_virtcachesize.eps
./eventdump.py ./stripped3 | sort -n |./virtcachesize.py stripped3_virtcachesize.eps
./eventdump.py ./stripped4 | sort -n |./virtcachesize.py stripped4_virtcachesize.eps
./eventdump.py ./stripped5 | sort -n |./virtcachesize.py stripped5_virtcachesize.eps
