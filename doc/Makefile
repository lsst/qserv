BUILDDIR = _build

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  init       to install Python dependencies"
	@echo "  clean      delete the exisiting build"
	@echo "  html       to make standalone HTML files"
	@echo "  linkcheck  to check all external links for integrity"
	@echo "  help       this command"


.PHONY: init
init:
	pip install -U -r requirements.txt

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: html
html:
	sphinx-build -b html -d $(BUILDDIR)/doctrees -n -W . $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: linkcheck
linkcheck:
	sphinx-build -b linkcheck  -d $(BUILDDIR)/doctrees -n -W . $(BUILDDIR)/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in $(BUILDDIR)/linkcheck/output.txt."
