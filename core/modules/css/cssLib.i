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
#include "css/CssError.h"
#include "css/KvInterface.h"
#include "css/KvInterfaceImplMem.h"
#include "css/KvInterfaceImplMySql.h"
#include "global/constants.h"
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"
%}

%include typemaps.i
%include cstring.i
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"

// Instantiate types
namespace std {
    %template(StringVector) vector<string>;
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

class AuthError(CssError):
    pass

class ConnError(CssError):
    pass

class KeyExistsError(CssError):
    pass

class BadAllocError(CssError):
    pass

class VersionMissingError(CssError):
    pass

class VersionMismatchError(CssError):
    pass
 %}

%include "sql/SqlErrorObject.h"

%{
    // RAII-style struct used to manage the ref-counted lifecycle of PyObject pointers
    struct PyObjectMgr {
        PyObjectMgr(PyObject* iObj) : obj(iObj) {
        }

        ~PyObjectMgr() {
            if (obj) {
                Py_DECREF(obj);
            }
        }

        // Used to check if object contains a valid pointer (inside if-statements)
        operator bool() const {
            return obj != nullptr;
        }

        // Convenience operator so that PyObjectMgr object may be passed to functions
        // that take PyObject* and the implicit pointer extraction will happen automatically.
        operator PyObject*() const {
            return obj;
        }

        PyObject* obj;
    };

    void setPythonException(const std::string& exceptionStr, const lsst::qserv::css::CssError& ex) {
        PyObjectMgr module(PyImport_ImportModule("lsst.qserv.css"));
        if (not module)
            return;
        PyObjectMgr exception(PyObject_GetAttrString(module, exceptionStr.c_str()));
        if (not exception)
            return;
        PyErr_SetString(exception, ex.what());
    }
%}

%exception {
    try {
        $action
    } catch (lsst::qserv::css::NoSuchDb e) {
        setPythonException("NoSuchDb", e);
        SWIG_fail;
    } catch (lsst::qserv::css::NoSuchKey e) {
        setPythonException("NoSuchKey", e);
        SWIG_fail;
    } catch (lsst::qserv::css::NoSuchTable e) {
        setPythonException("NoSuchTable", e);
        SWIG_fail;
    } catch (lsst::qserv::css::AuthError e) {
        setPythonException("AuthError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::ConnError e) {
        setPythonException("ConnError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::KeyExistsError e) {
        setPythonException("KeyExistsError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::BadAllocError e) {
        setPythonException("BadAllocError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::VersionMissingError e) {
        setPythonException("VersionMissingError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::VersionMismatchError e) {
        setPythonException("VersionMismatchError", e);
        SWIG_fail;
    } catch (lsst::qserv::css::CssError e) {
        // Important: the base class handler must be last
        setPythonException("CssError", e);
        SWIG_fail;
    }
}


%include "css/constants.h"
%include "css/KvInterface.h"
%include "css/KvInterfaceImplMem.h"

// must be included before KvInterfaceImplMySql
%include "global/stringTypes.h"
%include "mysql/MySqlConfig.h" 
%include "sql/SqlConnection.h"

%include "css/KvInterfaceImplMySql.h"
%include "global/constants.h"
