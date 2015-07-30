all: stripped1_modules.eps stripped2_modules.eps stripped3_modules.eps stripped4_modules.eps

%.eps : %.dot
	fdp -Tps $< > $@

%_modules.dot : ../step1/%
	./flow2dot.py  $< > $@

