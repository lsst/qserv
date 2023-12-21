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
#ifndef LSST_QSERV_REPLICA_INGESTRESOURCEMGRT_H
#define LSST_QSERV_REPLICA_INGESTRESOURCEMGRT_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "replica/ingest/IngestResourceMgr.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * IngestResourceMgrT is a fully transient implementation of the interface
 * class IngestResourceMgr. It's meant to be used for unit testing.
 */
class IngestResourceMgrT : public IngestResourceMgr {
public:
    /**
     * The factory method for instances of the class.
     * @return std::shared_ptr<IngestResourceMgr>
     */
    static std::shared_ptr<IngestResourceMgrT> create();

    IngestResourceMgrT(IngestResourceMgrT const&) = delete;
    IngestResourceMgrT& operator=(IngestResourceMgrT const&) = delete;

    virtual ~IngestResourceMgrT() = default;

    /// @see IngestResourceMgr::asyncProcLimit()
    virtual unsigned int asyncProcLimit(std::string const& databaseName) const;

    /**
     * Set/reset the limit for the number of asyncronous processing requests.
     *
     * @param databaseName This parameter defines the name of a database for which
     *  the limit is being set.
     * @param limit A value of the limit. If 0 is passed as a value of the limit
     *  then the previously set limit (if any) will be eliminated.
     * @throws std::invalid_argument If the empty string was passed as the name
     *  of a database, or if no such database was found.
     */
    void setAsyncProcLimit(std::string const& databaseName, unsigned int limit);

private:
    IngestResourceMgrT() = default;

    /// The mutex for enforcing thread safety of the class's public API and
    /// internal operations.
    mutable std::mutex _mtx;

    std::map<std::string, unsigned int> _asyncProcLimit;
};
}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTRESOURCEMGRT_H
