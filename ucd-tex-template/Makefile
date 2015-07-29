main.pdf: conf.tex main.tex abstract.tex introduction.tex literature.tex problem.tex approach.tex results.tex evaluation.tex references.tex appendix1.tex
	pdflatex main
	#Run a second time to make sure our TOC is up to date
	pdflatex main
clean:
	rm *.log *.aux *.pdf *.toc
