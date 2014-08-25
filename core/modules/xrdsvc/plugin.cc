// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
/// Implement XrdSsiGetServerService() to provide Qserv's SsiService
/// implementation. Link this file when building a plugin to be used as
/// ssi.svclib .


// System headers
#include <iostream>

// Third-party headers
#include "XrdSsi/XrdSsiLogger.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "xrdsvc/SsiService.h"

class XrdSsiLogger;
class XrdSsiCluster;

extern "C" {
XrdSsiService *XrdSsiGetServerService(XrdSsiLogger  *logP,
                                      XrdSsiCluster *clsP,
                                      const char    *cfgFn,
                                      const char    *parms)
{
    std::cerr << " Returning new Service " << std::endl;
    logP->Msg("pfx", "Hello");
    return new lsst::qserv::xrdsvc::SsiService(logP);
}
} // extern "C"
