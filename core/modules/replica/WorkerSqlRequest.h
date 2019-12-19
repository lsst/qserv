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
#ifndef LSST_QSERV_REPLICA_WORKERSQLREQUEST_H
#define LSST_QSERV_REPLICA_WORKERSQLREQUEST_H

// System headers
#include <string>

// Qserv headers
#include "replica/protocol.pb.h"
#include "replica/DatabaseMySQL.h"
#include "replica/WorkerRequest.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {
    class Connection;
}}}}} // Forward declaration

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerSqlRequest executes queries against the worker database
 * and return results sets (if any) back to a caller.
 *
 * @note Queries passed into this operation are supposed to be well formed.
 * If a MySQL error would occur during an attempt to execute an incorrectly
 * formed query then the corresponding MySQL error will be recorded
 * and reported to a caller in the response structure which is set
 * by method WorkerSqlRequest::setInfo().
 */
class WorkerSqlRequest : public WorkerRequest {
public:
    typedef std::shared_ptr<WorkerSqlRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider It's needed to access the Configuration of a setup
     *   and for validating the input parameters
     * @param worker The name of a worker. The name must match the worker which
     *   is going to execute the request.
     * @param id An identifier of a client request
     * @param priority indicates the importance of the request
     * @param (optional) onExpired request expiration callback function.
     *   If nullptr is passed as a parameter then the request will never expire.
     * @param (optional) requestExpirationIvalSec request expiration interval.
     *   If 0 is passed into the method then a value of the corresponding
     *   parameter for the Controller-side requests will be pulled from
     *   the Configuration.
     * @param request The ProtoBuf body of the original request
     * @return A pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec,
                      ProtocolRequestSql const& request);

    WorkerSqlRequest() = delete;
    WorkerSqlRequest(WorkerSqlRequest const&) = delete;
    WorkerSqlRequest& operator=(WorkerSqlRequest const&) = delete;

    ~WorkerSqlRequest() override = default;

    /// @return the original request
    ProtocolRequestSql const& request() const { return _request; }

    /**
     * Extract request status into the Protobuf response object.
     * @param response The Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseSql& response) const;

    bool execute() override;

private:
    WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                     std::string const& worker,
                     std::string const& id,
                     int priority,
                     ExpirationCallbackType const& onExpired,
                     unsigned int requestExpirationIvalSec,
                     ProtocolRequestSql const& request);

    /// @return A connector as per the input request
    std::shared_ptr<database::mysql::Connection> _connector() const;

    /**
     * The query generator for simple requests uses parameters of a request
     * to compose a desired query.
     * @note this method is only used to generate a single query for
     *   the non-batch requests.
     * @param conn A reference to the database connector is needed to process
     *   arguments to meet requirements of the database query processing engine.
     * @return A query as per the input request.
     * @throws std::invalid_argument For unsupported requests types supported.
     */
    std::string _query(std::shared_ptr<database::mysql::Connection> const& conn) const;

    /**
     * An alternative query generator for the "batch" requests for the specified
     * table. The generator uses parameters of a request and the name of a table
     * to compose a desired query. Only a subset of requests can be
     * executed in the "batch" mode.
     * @param conn A reference to the database connector is needed to process
     *   arguments to meet requirements of the database query processing engine.
     * @param table The name of table affected by the query.
     * @return A query as per the input request and the name of a table.
     * @throws std::invalid_argument For unsupported requests types.
     */
    std::string _batchQuery(database::mysql::Connection::Ptr const& conn,
                            std::string const& table) const;

    /**
     * Extract a result set (if any) via the database connector into
     * the Protobuf response object.
     * @param lock The lock must be held before calling the method since it's
     *   going to access a protected state of the object.
     * @param conn  a valid database connector for extracting a result set
     */
    void _extractResultSet(util::Lock const& lock,
                           std::shared_ptr<database::mysql::Connection> const& conn);

    /**
     * Report & record a failure
     *
     * @param lock The lock must be held before calling the method since it's
     *   going to modify a protected state of the object.
     * @param statusExt An extended status to be reported to Controllers and
     *   set in the current (most recently processed query if any) result set.
     * @param error A message to be logged and returned to Controllers.
     * @throws std::logic_error Is thrown when the method is called before
     *   creating a result set.
     */    
    void _reportFailure(util::Lock const& lock,
                        ExtendedCompletionStatus statusExt,
                        std::string const& error);

    // @return A mutable pointer to the current result set
    ProtocolResponseSqlResultSet* _currentResultSet(util::Lock const& lock);

    /// @return 'true' if the request issues multiple queries
    bool _batchMode() const;

    // Input parameters

    ProtocolRequestSql const _request;

    /// Cached result to be sent to a client upon a request
    mutable ProtocolResponseSql _response;
};

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestFS;

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestPOSIX;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERSQLREQUEST_H
