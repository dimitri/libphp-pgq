PKGNAME = libphp-pgq
VERSION = $(shell awk -F '[-()]' 'NR == 1 {print $$3}' debian/changelog)
DOCS    = README
PHP_SRC = $(wildcard *.php)

DESTDIR =
libdir  = $(DESTDIR)/usr/share/php/pgq
docdir  = $(DESTDIR)/usr/share/doc/libphp-pgq

DEBDIR = /tmp/$(PKGNAME)
EXPORT = $(DEBDIR)/export/$(PKGNAME)-$(VERSION)
ORIG   = $(DEBDIR)/export/$(PKGNAME)_$(VERSION).orig.tar.gz
ARCHIVE= $(DEBDIR)/export/$(PKGNAME)-$(VERSION).tar.gz

doc: README.html

README.html: README.asciidoc
	asciidoc -a toc -o $@ $<

install: doc
	install -d $(libdir)
	cp -a $(PHP_SRC) $(libdir)

	install -d $(docdir)
	cp $(DOCS) $(docdir)
	cp -a examples $(docdir)

deb:
	# working copy from where to make the .orig archive
	rm -rf $(DEBDIR)	
	mkdir -p $(DEBDIR)/$(PKGNAME)-$(VERSION)
	mkdir -p $(EXPORT)
	cp -a . $(EXPORT)

	# get rid of temp and build files
	for n in ".#*" "*~" "build-stamp" "configure-stamp"; do \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 echo rm -f; \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 rm -f; \
	done

	# get rid of CVS dirs
	for n in "CVS" "CVSROOT"; do \
	  find $(EXPORT) -type d -name "$$n" -print0|xargs -0 rm -rf; \
	  find $(EXPORT) -type d -name "$$n" -print0|xargs -0 rm -rf; \
	done

	# prepare the .orig without the debian/ packaging stuff
	cp -a $(EXPORT) $(DEBDIR)
	rm -rf $(DEBDIR)/$(PKGNAME)-$(VERSION)/debian
	(cd $(DEBDIR) && tar czf $(ORIG) $(PKGNAME)-$(VERSION))

	# have a copy of the $ORIG file named $ARCHIVE for non-debian packagers
	cp $(ORIG) $(ARCHIVE)

	# build the debian package and copy them to ..
	(cd $(EXPORT) && debuild -us -uc)
	cp -a $(DEBDIR)/export/$(PKGNAME)[_-]$(VERSION)??[._]* ..
	cp -a $(ARCHIVE) ..
	cp -a $(ORIG) ..
