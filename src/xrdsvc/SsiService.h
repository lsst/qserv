// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 LSST Corporation.
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

#ifndef LSST_QSERV_XRDSVC_SSISERVICE_H
#define LSST_QSERV_XRDSVC_SSISERVICE_H

// System headers
#include <memory>

// Third-party headers
#include "XrdSsi/XrdSsiResource.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "mysql/MySqlConfig.h"

// Forward declarations
class XrdSsiLogger;

namespace lsst::qserv {
namespace util {
class FileMonitor;
}
namespace wcontrol {
class Foreman;
}  // namespace wcontrol
namespace wpublish {
class ChunkInventory;
}
}  // namespace lsst::qserv

namespace lsst::qserv::xrdsvc {

/// SsiService is an XrdSsiService implementation that implements a Qserv query
/// worker services
class SsiService : public XrdSsiService {
public:
    /** Build a SsiService object
     * @param log xrdssi logger
     * @note take ownership of logger for now
     */

    SsiService(XrdSsiLogger* log);
    virtual ~SsiService();

    /// Called by SSI framework to handle new requests
    void ProcessRequest(XrdSsiRequest& reqRef, XrdSsiResource& resRef) override;

private:
    void _initInventory();
    void _configure();

    /// List of available chunks.
    std::shared_ptr<wpublish::ChunkInventory> _chunkInventory;

    // The Foreman contains essential structures for adding and running tasks.
    std::shared_ptr<wcontrol::Foreman> _foreman;

    mysql::MySqlConfig const _mySqlConfig;

    /// Reloads the log configuration file on log config file change.
    std::shared_ptr<util::FileMonitor> _logFileMonitor;

};  // class SsiService

}  // namespace lsst::qserv::xrdsvc

#endif  // LSST_QSERV_XRDSVC_SSISERVICE_H
