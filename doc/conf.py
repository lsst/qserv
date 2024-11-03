from documenteer.conf.guide import *

import contextlib
import os
import re

from documenteer.sphinxconfig.utils import form_ltd_edition_name

# Add any paths that contain templates here, relative to this directory.
templates_path = ["templates"]

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

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["misc", "CMakeLists.txt"]

# Add any URL patterns to ignore (e.g. for private sites, or sites that
# are frequently down).
linkcheck_ignore = [
    r"^https://jira.lsstcorp.org/browse/",
    r"^https://dev.lsstcorp.org/trac",
    r"^https://confluence.lsstcorp.org/display/",
    r"^https://rubinobs.atlassian.net/wiki/",
    r"^https://rubinobs.atlassian.net/browse/",
    r"^https://www.slac.stanford.edu/",
]

html_additional_pages = {
    "index": "overview.html"
}

mermaid_version = "10.3.0"
