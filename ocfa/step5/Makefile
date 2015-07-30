all: stripped1_modules.eps stripped2_modules.eps stripped3_modules.eps stripped4_modules.eps stripped1_modtypes.eps stripped2_modtypes.eps stripped3_modtypes.eps stripped4_modtypes.eps

%.eps : %.dot
	fdp -Tps $< > $@

%_modules.dot : ../step1/%
	./flow2dot.py  $< > $@

%_modtypes.dot : ../step1/%
	./genflow2dot.py $< > $@
