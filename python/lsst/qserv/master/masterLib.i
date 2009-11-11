// -*- lsst-c++ -*-
%define qserv_master_DOCSTRING
"
Access to the classes from the qserv_master library
"
%enddef

 //%feature("autodoc", "1");
 //%module("threads"=1, package="lsst.qserv.master", docstring=qserv_master_DOCSTRING) masterLib
%module("threads"=1, package="lsst.qserv.master") masterLib
%{
#include "lsst/qserv/master/xrdfile.h"
%}

// %include "lsst/p_lsstSwig.i"
%include typemaps.i
%include cstring.i
%include carrays.i
%include cdata.i
%array_class(char, charArray);

// %lsst_exceptions()
// %import "lsst/pex/exceptions/exceptionsLib.i"

// Define any smart pointers here:
// SWIG_SHARED_PTR(Persistable, lsst::daf::base::Persistable)
// SWIG_SHARED_PTR_DERIVED(PropertySet, lsst::daf::base::Persistable, lsst::daf::base::PropertySet)

// Include all classes to wrap:
// %include "lsst/qserv/master/Master.h"

 //%apply (void *STRING, unsigned long long LENGTH) { (void *buf, unsigned long long nbyte) };
%apply (char *STRING, int LENGTH) { (char *str, int len) };
%include "lsst/qserv/master/xrdfile.h"

// Instantiate any templates here:
// %template(setBool) lsst::daf::base::PropertySet::set<bool>;

