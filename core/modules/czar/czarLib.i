// -*- lsst-c++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 AURA/LSST.
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

%define qserv_czar_DOCSTRING
"
Access to the classes from the qserv_czar library
"
%enddef

%ignore lsst::qserv::query::operator<<;
%ignore lsst::qserv::qproc::operator<<;

%ignore lsst::qserv::qdisp::TransactionSpec::Reader;

//%feature("autodoc", "1");
//%module("threads"=1, package="lsst.qserv.czar", docstring=qserv_czar_DOCSTRING) czarLib
%module("threads"=1, package="lsst.qserv.czar") czarLib
%{
#define SWIG_FILE_WITH_INIT
#include "ccontrol/dispatcher.h"
#include "ccontrol/queryMsg.h"
#include "ccontrol/QueryState.h"
#include "css/KvInterface.h"
#include "css/KvInterfaceImplMem.h"
#include "ccontrol/UserQueryFactory.h"
#include "ccontrol/userQueryProxy.h"
#include "css/StripingParams.h"
#include "global/constants.h"
#include "log/loggerInterface.h"
#include "qdisp/ChunkMeta.h"
#include "qdisp/TransactionSpec.h"
#include "qproc/ChunkSpec.h"
#include "rproc/mergeTypes.h"
#include "util/common.h"
#include "util/Substitution.h"
#include "xrdc/xrdfile.h"
%}

// %include "lsst/p_lsstSwig.i"
%include typemaps.i
%include cstring.i
%include carrays.i
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"

%include cdata.i
%array_class(char, charArray);

// %lsst_exceptions()
// %import "lsst/pex/exceptions/exceptionsLib.i"

// Instantiate the map we need
namespace std {
    %template(StringMap) map<std::string, std::string>;
};

// ------------------------------------------------------------------------
// Copied from http://www.swig.org/Doc1.3/Python.html#Python_nn59

// This tells SWIG to treat char ** as a special case
%typemap(in) char ** {
  /* Check if is a list */
  if (PyList_Check($input)) {
    int size = PyList_Size($input);
    int i = 0;
    $1 = (char **) malloc((size+1)*sizeof(char *));
    for (i = 0; i < size; i++) {
      PyObject *o = PyList_GetItem($input,i);
      if (PyString_Check(o))
	$1[i] = PyString_AsString(PyList_GetItem($input,i));
      else {
	PyErr_SetString(PyExc_TypeError,"list must contain strings");
	free($1);
	return NULL;
      }
    }
    $1[i] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError,"not a list");
    return NULL;
  }
}

// This cleans up the char ** array we malloc'd before the function call
%typemap(freearg) char ** {
  free((char *) $1);
}

// ------------------------------------------------------------------------

%include "boost_shared_ptr.i"
%shared_ptr(lsst::qserv::css::KvInterface)
%shared_ptr(lsst::qserv::css::KvInterfaceImplMem)

// Include all classes to wrap:
// %include "lsst/qserv/czar/Master.h"

//%apply (void *STRING, unsigned long long LENGTH) { (void *buf, unsigned long long nbyte) };
%apply (char *STRING, int LENGTH) { (char *str, int len) };
//%apply (const char *STRING, int LENGTH) { (const char *str, int len) };
%apply int *OUTPUT { int *write, int *read };
%apply int *OUTPUT { int* chunkId, int* code, time_t* timestamp };

%include "ccontrol/dispatcher.h"
%include "ccontrol/queryMsg.h"
%include "ccontrol/queryMsg.h"
%include "ccontrol/QueryState.h"
%include "css/KvInterface.h"
%include "css/KvInterfaceImplMem.h"
%include "ccontrol/UserQueryFactory.h"
%include "ccontrol/userQueryProxy.h"
%include "css/StripingParams.h"
%include "global/constants.h"
%include "log/loggerInterface.h"
%include "qdisp/ChunkMeta.h"
%include "qdisp/TransactionSpec.h"
%include "qproc/ChunkSpec.h"
%include "query/Constraint.h"
%include "rproc/mergeTypes.h"
%include "util/Substitution.h"
%include "xrdc/xrdfile.h"

// Instantiate any templates here:
// %template(setBool) lsst::daf::base::PropertySet::set<bool>;

