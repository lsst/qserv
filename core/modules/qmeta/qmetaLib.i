// -*- lsst-c++ -*-

%define qserv_qmeta_DOCSTRING
"
Access to the qmeta classes
"
%enddef

%module("threads"=1, package="lsst.qserv.qmeta") qmetaLib
%{
#define SWIG_FILE_WITH_INIT
#include "qmeta/Exceptions.h"
#include "qmeta/QInfo.h"
#include "qmeta/QMeta.h"
#include "qmeta/types.h"
%}

%include typemaps.i
%include cstring.i
%include "std_map.i"
%include "std_shared_ptr.i"
%include "std_string.i"
%include "std_vector.i"

// we need few types from stdint.h but SWIG provides incorrect typedef
// for 64-bit types in its stdint.i so we cannot use that, need some manual defs
namespace std {
typedef int                     int32_t;
typedef long int                int64_t;
typedef unsigned int            uint32_t;
typedef unsigned long int       uint64_t;
}

// Instantiate types
%template(StringMap) std::map<std::string, std::string>;

%pythoncode %{
_Exception = Exception
class Exception(_Exception):
    pass

class CzarNameError(Exception):
    pass

class CzarIdError(Exception):
    pass

class QueryIdError(Exception):
    pass

class ChunkIdError(Exception):
    pass

class SqlError(Exception):
    pass

class MissingTableError(Exception):
    pass

class ConsistencyError(Exception):
    pass
%}

%{
    void setPythonException(const lsst::qserv::qmeta::Exception& ex) {
        swig::SwigPtr_PyObject module(PyImport_ImportModule("lsst.qserv.qmeta"), false);
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
    } catch (lsst::qserv::qmeta::Exception const& exc) {
        setPythonException(exc);
        SWIG_fail;
    }
}

%shared_ptr(lsst::qserv::qmeta::QMeta)

%include "qmeta/types.h"
%template(QueryIdList) std::vector<lsst::qserv::qmeta::QueryId>;
%include "qmeta/QInfo.h"
%include "qmeta/QMeta.h"
