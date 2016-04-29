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

#ifndef LSST_QSERV_XRDSVC_SSIPROVIDER_H
#define LSST_QSERV_XRDSVC_SSIPROVIDER_H

// System headers
#include <memory>
#include <string>

// Third-party headers
#include "XrdSsi/XrdSsiProvider.hh"

// Qserv headers
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/SsiService.h"

// Forward declarations
class XrdSsiCluster;
class XrdSsiLogger;

namespace lsst {
namespace qserv {
namespace xrdsvc {

class SsiProviderServer : public XrdSsiProvider
{
public:

    virtual XrdSsiService *GetService(XrdSsiErrInfo& eInfo,
                                      char const*    contact,
                                      int            oHold=256) override {
        return _service.get();
    }

    virtual bool  Init(XrdSsiLogger* logP,  XrdSsiCluster* clsP,
                       char const*   cfgFn, char const*    parms,
                       int           argc,  char**         argv) override;

    virtual rStat QueryResource(char const* rName,
                                char const* contact=0) override;

                  SsiProviderServer() : _cmsSsi(0), _logSsi(0) {}
    virtual      ~SsiProviderServer();

private:

    wpublish::ChunkInventory _chunkInventory;
    std::unique_ptr<SsiService> _service;

    XrdSsiCluster* _cmsSsi;
    XrdSsiLogger*  _logSsi;
};

}}} // lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_SSIPROVIDER_H
