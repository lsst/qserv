// -*- lsst-c++ -*-
%define qserv_worker_DOCSTRING
"
Access to the classes from the qserv_worker library
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.qserv.worker", docstring=qserv_worker_DOCSTRING) workerLib

%{
%}

%include "lsst/p_lsstSwig.i"

%lsst_exceptions()
%import "lsst/pex/exceptions/exceptionsLib.i"

// Define any smart pointers here:
// SWIG_SHARED_PTR(Persistable, lsst::daf::base::Persistable)
// SWIG_SHARED_PTR_DERIVED(PropertySet, lsst::daf::base::Persistable, lsst::daf::base::PropertySet)

// Include all classes to wrap:
// %include "lsst/qserv/worker/Worker.h"

// Instantiate any templates here:
// %template(setBool) lsst::daf::base::PropertySet::set<bool>;

