# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
# export_deps.py assists exporting a library/header's dependencies
# so that clients may use them without finding/detecting the implicit
# dependencies on their own.

def installWithDeps(env, sourceObj, logicalName, targetPath, depDict):
    """Install a file/obj along with a constructed dep file.
    Builds a dependency file for sourceObj and installs sourceObj 
    and its dep file to a target path.

    sourceObj and logical name are both needed. sourceObj is needed to
    satisfy env.Install(...) and logical name is needed to construct 
    the dep file name, which is based on the logical name (lacking 
    platform-specific prefix/suffix) and cannot be easily deduced from 
    sourceObj alone. In the case of headers, they are often the same.
    depDict : dict containing LIBPATH, CPPPATH, LIBS that are needed.

    Example:
    lib = pEnv.Library("qserv_proto", sources)
    
    deps = {'LIBPATH' : [os.path.abspath(os.environ["PROTOC_LIB"])],
            'LIBS' : ['protobuf','ssl']}
    tup = export_deps.installWithDeps(env, lib, "qserv_proto", "../lib", deps)
    env.Append(built_libs=[tup[0]])"""

    def makeDepFile(target, source, env):
        dContents = str(depDict)
        open(str(target[0]),"w").write(dContents)
    targetFile = logicalName + ".deps"
    dep = env.Command(targetFile, sourceObj, makeDepFile)
    obj = env.Install(targetPath, sourceObj)
    depInst = env.Install(targetPath, dep);
    return (obj, dep, depInst)


