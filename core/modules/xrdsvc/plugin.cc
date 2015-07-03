// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
/// Implement XrdSsiProviderServer to provide Qserv's SsiService
/// implementation. Link this file when building a plugin to be used as
/// ssi.svclib.

// System headers
#include <memory>

// Third-party headers
#include "XrdSsi/XrdSsiProvider.hh"

// Qserv headers
#include "SsiService.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

class SsiProviderServer : public XrdSsiProvider
{
public:

    virtual XrdSsiService *GetService(XrdSsiErrInfo& eInfo, char const* contact, int oHold=256) {
        return _service.get();
    }

    virtual bool Init(XrdSsiLogger* logP, XrdSsiCluster* clsP, char const* cfgFn,
            char const* parms, int argc, char **argv) {
        _service = std::unique_ptr<SsiService>(new SsiService(logP));
        return true;
    }

    virtual rStat QueryResource(char const*rName, char const* contact=0) {
        // Not called on this object but part of pure virtual interface,
        // so we need to provide at least this dummy implementation.
        return isPresent;
    }

private:

    std::unique_ptr<SsiService> _service;

};

}}} // lsst::qserv::xrdsvc

XrdSsiProvider *XrdSsiProviderServer = new lsst::qserv::xrdsvc::SsiProviderServer;
