main.pdf: main.tex conf.tex ucd-tex-template/abstract.tex ucd-tex-template/introduction.tex ucd-tex-template/literature.tex ucd-tex-template/problem.tex ucd-tex-template/approach.tex ucd-tex-template/results.tex ucd-tex-template/evaluation.tex ucd-tex-template/references.tex ucd-tex-template/appendix1.tex
	pdflatex main
	#Run a second time to make sure our TOC is up to date
	pdflatex main
clean:
	rm *.log *.aux *.pdf *.toc
