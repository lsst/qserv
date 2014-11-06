#
# LSST Data Management System
# Copyright 2014 LSST/AURA.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
# pytarget.py -- helpers for modules to publish python modules
import os
import state

def getDefaultTargets(env, path, ignore=lambda f:False):
    """ 
    @param env   a SCons environment
    @param path  relative path to the module
    @param ignore a function that returns True if a file should be ignored.
    """
    files = filter(lambda f: not (os.path.basename(str(f)).startswith("test")
                                  or ignore(f)),
                   env.Glob(os.path.join(path, "*.cc")))
    files.sort(key=lambda n: n.str_for_display())
    return files
