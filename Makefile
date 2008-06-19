
doc: README.html

README.html: README
	asciidoc -a toc -o $@ $<