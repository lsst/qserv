// -*- lsst-c++ -*-
%define qserv_master_DOCSTRING
"
Access to the classes from the qserv_master library
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.qserv.master", docstring=qserv_master_DOCSTRING) masterLib

%include "lsst/p_lsstSwig.i"

%lsst_exceptions()
%import "lsst/pex/exceptions/exceptionsLib.i"

// Define any smart pointers here:
// SWIG_SHARED_PTR(Persistable, lsst::daf::base::Persistable)
// SWIG_SHARED_PTR_DERIVED(PropertySet, lsst::daf::base::Persistable, lsst::daf::base::PropertySet)

// Include all classes to wrap:
// %include "lsst/qserv/master/Master.h"

// Instantiate any templates here:
// %template(setBool) lsst::daf::base::PropertySet::set<bool>;

