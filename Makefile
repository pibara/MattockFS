ucd_fcci_rob_j_meijer.pdf:  main.tex conf.tex ucd-tex-template/abstract.tex ucd-tex-template/introduction.tex ucd-tex-template/literature.tex ucd-tex-template/problem.tex ucd-tex-template/approach.tex ucd-tex-template/results.tex ucd-tex-template/evaluation.tex ucd-tex-template/references.tex ocfa/step2/results.tex ocfa/conclussions.tex ocfa/step4/results.tex ocfa/intro.tex ocfa/step3/results.tex ocfa/step5/results.tex
	pdflatex -jobname ucd_fcci_rob_j_meijer main
	#Run a second time to make sure our TOC is up to date
	pdflatex -jobname ucd_fcci_rob_j_meijer main
clean:
	rm *.log *.aux *.pdf *.toc
	rm ocfa/*.aux
	rm ocfa/step*/*.aux ocfa/step*/*.pdf
