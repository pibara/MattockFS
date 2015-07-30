all: stripped1_virtcachesize.eps stripped2_virtcachesize.eps stripped3_virtcachesize.eps stripped4_virtcachesize.eps

stripped1_virtcachesize.eps: ../step1/stripped1
	./eventdump.py ../step1/stripped1 | sort -n |./virtcachesize.py stripped1_virtcachesize.eps

stripped2_virtcachesize.eps: ../step1/stripped2
	./eventdump.py ../step1/stripped2 | sort -n |./virtcachesize.py stripped2_virtcachesize.eps

stripped3_virtcachesize.eps: ../step1/stripped3
	./eventdump.py ../step1/stripped3 | sort -n |./virtcachesize.py stripped3_virtcachesize.eps

stripped4_virtcachesize.eps: ../step1/stripped4
	./eventdump.py ../step1/stripped4 | sort -n |./virtcachesize.py stripped4_virtcachesize.eps

