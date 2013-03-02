// -*- lsst-c++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
%define qserv_master_DOCSTRING
"
Access to the classes from the qserv_master library
"
%enddef

 //%feature("autodoc", "1");
 //%module("threads"=1, package="lsst.qserv.master", docstring=qserv_master_DOCSTRING) masterLib
%module("threads"=1, package="lsst.qserv.master") masterLib
%{
#define SWIG_FILE_WITH_INIT
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/dispatcher.h"
#include "lsst/qserv/master/mergeTypes.h"
#include "lsst/qserv/master/Substitution.h"
#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/ChunkMeta.h"
#include "lsst/qserv/master/SqlSubstitution.h"
#include "lsst/qserv/master/TableMerger.h"
#include "lsst/qserv/master/common.h"
#include "lsst/qserv/master/ifaceMeta.h"%}


// %include "lsst/p_lsstSwig.i"
%include typemaps.i
%include cstring.i
%include carrays.i  
%include "std_map.i"
%include "std_string.i"

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
%module argv

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



// Define any smart pointers here:
// SWIG_SHARED_PTR(Persistable, lsst::daf::base::Persistable)
// SWIG_SHARED_PTR_DERIVED(PropertySet, lsst::daf::base::Persistable, lsst::daf::base::PropertySet)

// Include all classes to wrap:
// %include "lsst/qserv/master/Master.h"

 //%apply (void *STRING, unsigned long long LENGTH) { (void *buf, unsigned long long nbyte) };
%apply (char *STRING, int LENGTH) { (char *str, int len) };
//%apply (const char *STRING, int LENGTH) { (const char *str, int len) };
%apply int *OUTPUT { int *write, int *read };
%include "lsst/qserv/master/transaction.h"
%include "lsst/qserv/master/xrdfile.h"
%include "lsst/qserv/master/mergeTypes.h"
%include "lsst/qserv/master/dispatcher.h"
%include "lsst/qserv/master/Substitution.h"
%include "lsst/qserv/master/ChunkMeta.h"
%include "lsst/qserv/master/ChunkMapping.h"
%include "lsst/qserv/master/SqlSubstitution.h"
%include "lsst/qserv/master/TableMerger.h"
%include "lsst/qserv/master/ifaceMeta.h"

// Instantiate any templates here:
// %template(setBool) lsst::daf::base::PropertySet::set<bool>;

