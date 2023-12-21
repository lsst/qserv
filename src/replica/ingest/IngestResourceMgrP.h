/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_INGESTRESOURCEMGRP_H
#define LSST_QSERV_REPLICA_INGESTRESOURCEMGRP_H

// System headers
#include <map>
#include <memory>
#include <string>

// Qserv headers
#include "replica/ingest/IngestResourceMgr.h"

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * IngestResourceMgrP is an implementation of the interface class IngestResourceMgr
 * that has persistent backend. It's meant to be used in production.
 *
 * The implementation will pull values of the limits from the database.
 */
class IngestResourceMgrP : public IngestResourceMgr {
public:
    /**
     * The factory method for instances of the class.
     * @param serviceProvider Provider of various services (database, configuration, etc.).
     * @return std::shared_ptr<IngestResourceMgr>
     */
    static std::shared_ptr<IngestResourceMgrP> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider);

    IngestResourceMgrP(IngestResourceMgrP const&) = delete;
    IngestResourceMgrP& operator=(IngestResourceMgrP const&) = delete;

    virtual ~IngestResourceMgrP() = default;

    /// @see IngestResourceMgr::asyncProcLimit()
    virtual unsigned int asyncProcLimit(std::string const& databaseName) const;

private:
    IngestResourceMgrP(std::shared_ptr<ServiceProvider> const& serviceProvider);

    std::shared_ptr<ServiceProvider> const _serviceProvider;
};
}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTRESOURCEMGRP_H
