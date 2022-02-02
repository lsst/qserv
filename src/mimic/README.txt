The mimic module has small amounts of python code copied from modules that we do
not want to import all of, because of unused indirect dependencies, or
installation time or size.

For example, lsst.log depends on a couple functions in lsst.utils, and
lsst.utils depends on numpy, but lsst.log does not indirectly depend on numpy;
i.e. lsst.log does not use functions in lsst.utils that depend on numpy. Since
the amount of code used by lsst.log in lsst.utils is small, we copy the code
here.
