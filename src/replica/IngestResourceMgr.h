
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
#ifndef LSST_QSERV_REPLICA_INGESTRESOURCEMGR_H
#define LSST_QSERV_REPLICA_INGESTRESOURCEMGR_H

// System headers
#include <memory>
#include <string>

// This header declarations
namespace lsst::qserv::replica {

/**
 * IngestResourceMgr is an interface class for a family of concrete implementations
 * of the resource managers employed by the class IngestRequestMgr for scheduling
 * request execution depending on the resource usage limits.
 *
 * @note Value of the limits may change over time. The request manager may
 *   test the limits before scheduling requests.
 *
 * @see class IngestRequestMgr
 */
class IngestResourceMgr : public std::enable_shared_from_this<IngestResourceMgr> {
public:
    virtual ~IngestResourceMgr() = default;

    /**
     * Return a limit for the number of the ingest requests allowed
     * to be executed concurrently. The limit applies to requests
     * submitted via the asynchronous interface.
     *
     * @param databaseName The name of database for which the limit is retrieved.
     * @return Return a value of the limit, or 0 if no limit was defined for
     *  the specific subject.
     * @throws std::invalid_argument If the empty string was passed as the name
     *  of a database.
     */
    virtual unsigned int asyncProcLimit(std::string const& databaseName) const = 0;

protected:
    /**
     * Throw an exception if the databaseName is an empty string.
     * @param func The name of a method that requested the test (used for error reporting).
     * @param databaseName A value to be validated.
     * @throws std::invalid_argument If the empty string was passed as the name
     *  of a database.
     */
    static void throwIfEmpty(std::string const& func, std::string const& databaseName);
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTRESOURCEMGR_H
