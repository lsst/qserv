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
import state # for qserv scons logging

def getPyTargets(env, pathList, extras):
    """@return list of py targets to be appended to extraTgts['dist']
    """
    python_libpath=os.path.join("lib", "python")
    pyDir = os.path.join(python_libpath,*pathList)
    srcPyDir = os.path.join("python", *pathList)

    state.log.debug("pathList, %s" % pathList)
    state.log.debug("srcPyDir %s" % srcPyDir)
    # return list of installable py files
    pathFiles = [(pyDir, s) for s in env.Glob(os.path.join(srcPyDir,"*.py"))]
    extraFiles = [(pyDir, s) for s in extras]
    return pathFiles + extraFiles
    # touch init files will be done by python builder used 
    # for deploying others python/lsst/qserv package. See
    # InstallPythonModule() in main SConstruct
