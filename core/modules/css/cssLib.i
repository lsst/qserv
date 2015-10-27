// -*- lsst-c++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

%define qserv_css_DOCSTRING
"
Access to the classes from the qserv_css library
"
%enddef

%module("threads"=1, package="lsst.qserv.css") cssLib
%{
#define SWIG_FILE_WITH_INIT
#include "css/constants.h"
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "css/KvInterface.h"
#include "global/intTypes.h"
%}

%include typemaps.i
%include cstring.i
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"

// Instantiate types
namespace std {
    %template(StringVector) vector<string>;
    %template(StringMap) map<string, string>;
    %template(NodeParamMap) map<string, lsst::qserv::css::NodeParams>;
    %template(ChunkMap) map<int, vector<string>>;
};


%pythoncode %{
class CssError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

class NoSuchDb(CssError):
    pass

class NoSuchKey(CssError):
    pass

class NoSuchTable(CssError):
    pass

class TableExists(CssError):
    pass

class AuthError(CssError):
    pass

class ConnError(CssError):
    pass

class KeyExistsError(CssError):
    pass

class KeyValueError(CssError):
    pass

class BadAllocError(CssError):
    pass

class VersionMissingError(CssError):
    pass

class VersionMismatchError(CssError):
    pass

class ReadonlyCss(CssError):
    pass

class NoSuchNode(CssError):
    pass

class NodeExists(CssError):
    pass

class NodeInUse(CssError):
    pass

class ConfigError(CssError):
    pass
 %}

%include "sql/SqlErrorObject.h"

%{
    void setPythonException(const lsst::qserv::css::CssError& ex) {
        swig::SwigPtr_PyObject module(PyImport_ImportModule("lsst.qserv.css"), false);
        if (not module)
            return;
        swig::SwigPtr_PyObject exception(PyObject_GetAttrString(module, ex.typeName().c_str()), false);
        if (not exception)
            return;
        PyErr_SetString(exception, ex.what());
    }
%}

%exception {
    try {
        $action
    } catch (lsst::qserv::css::CssError const& exc) {
        setPythonException(exc);
        SWIG_fail;
    }
}

%include "std_shared_ptr.i"
%shared_ptr(lsst::qserv::css::CssAccess)
%shared_ptr(lsst::qserv::css::KvInterface)

%include "css/constants.h"
%include "global/intTypes.h"
%include "css/EmptyChunks.h"
%include "css/KvInterface.h"
%include "css/MatchTableParams.h"
%include "css/NodeParams.h"
%include "css/PartTableParams.h"
%include "css/StripingParams.h"
%include "css/TableParams.h"
%include "css/CssAccess.h"
