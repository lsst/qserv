"""Sphinx configurations for the qserv.lsst.io documentation build."""

import contextlib
import os
import re
import sys

from documenteer.sphinxconfig.utils import form_ltd_edition_name

# -- General configuration ----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.intersphinx",
    "sphinx.ext.ifconfig",
    "documenteer.sphinxext",
]

source_suffix = ".rst"

root_doc = "index"

# General information about the project.
project = "Qserv"
copyright = "2016-2022 Association of Universities for Research in Astronomy, Inc. (AURA)"
author = "LSST Data Management"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.

# The short X.Y version.
github_ref = os.getenv("GITHUB_REF")
if github_ref is None:
    with contextlib.closing(os.popen('git symbolic-ref HEAD')) as p:
        github_ref = p.read().strip()
match = re.match(r"refs/(heads|tags|pull)/(?P<ref>.+)", github_ref)
if not match:
    git_ref = "main"
else:
    git_ref = match.group("ref")

version = form_ltd_edition_name(git_ref)

# The full version, including alpha/beta/rc tags.
release = version

html_last_updated_fmt = ""

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["CMakeLists.txt"]

# The reST default role cross-links Python (used for this markup: `text`)
default_role = "py:obj"

# -- Options for linkcheck builder --------------------------------------------

linkcheck_retries = 2

linkcheck_timeout = 15

# Add any URL patterns to ignore (e.g. for private sites, or sites that
# are frequently down).
linkcheck_ignore = [
    r"^https://jira.lsstcorp.org/browse/",
    r"^https://dev.lsstcorp.org/trac"
]

# -- Options for html builder -------------------------------------------------

html_theme = "sphinx_rtd_theme"

# Variables available for Jinja templates
html_context = {
    "display_github": True,
    "github_user": "lsst",
    "github_repo": "qserv",
    "github_version": git_ref + '/',
    "conf_py_path": "doc/"
}

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": -1
}

# The name for this set of Sphinx documents.  If unset, it defaults to
# "<project> v<release> documentation".
# html_title = ""

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = "Qserv"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

# If true, links to the reST sources are added to the pages.
# html_show_sourcelink = False

# -- Intersphinx --------------------------------------------------------------
# For linking to other Sphinx documentation.
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "pipelines": ("https://pipelines.lsst.io/", None),
}
