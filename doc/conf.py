from documenteer.conf.guide import *

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["misc", "CMakeLists.txt"]

# Add any URL patterns to ignore (e.g. for private sites, or sites that
# are frequently down).
linkcheck_ignore = [
    r"^https://jira.lsstcorp.org/",
    r"^https://dev.lsstcorp.org/trac",
    r"^https://confluence.lsstcorp.org/display/",
    r"^https://rubinobs.atlassian.net/wiki/",
    r"^https://rubinobs.atlassian.net/browse/",
    r"^https://www.slac.stanford.edu/",
    r".*/_images/",
    r"^https://dev.mysql.com/doc/refman/",
]

mermaid_version = "10.3.0"
