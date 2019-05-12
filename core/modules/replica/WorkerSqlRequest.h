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
 * @note
 *   queries passed into this operation are supposed to be well formed.
 *   If a MySQL error would occur during an attempt to execute an incorrectly
 *   formed query then the corresponding MySQL error will be recorded
 *   and report to a caller in the reponse structure which is set
 *   by method WorkerSqlRequest::setInfo().
 */
class WorkerSqlRequest : public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerSqlRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   provider is needed to access the Configuration of a setup
     *   and for validating the input parameters
     *
     * @param worker
     *   the name of a worker. The name must match the worker which
     *   is going to execute the request.
     * 
     * @param id
     *   an identifier of a client request
     *
     * @param request
     *   ProtoBuf body of the request
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      ProtocolRequestSql const& request);

    // Default construction and copy semantics are prohibited

    WorkerSqlRequest() = delete;
    WorkerSqlRequest(WorkerSqlRequest const&) = delete;
    WorkerSqlRequest& operator=(WorkerSqlRequest const&) = delete;

    ~WorkerSqlRequest() override = default;

    /// @return the original request
    ProtocolRequestSql const& request() const { return _request; }

    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseSql& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

private:

    /// @see WorkerSqlRequest::create()
    WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                     std::string const& worker,
                     std::string const& id,
                     ProtocolRequestSql const& request);

    /// @return a connector as per the input request
    std::shared_ptr<database::mysql::Connection> _connector() const;

    /**
     * The query generator uses parameters of a request to compose
     * a desired query.
     *
     * @param conn
     *   a reference to the database connector is needed to process arguments
     *   to meet requirements of the database query processing engine.
     *
     * @return
     *   a query as per the input request
     *
     * @throws std::invalid_argument
     *   if the input parameters are not supported
     */
    std::string _query(std::shared_ptr<database::mysql::Connection> const& conn) const;

    /**
     * Initialize the Protobuf response object.
     * 
     * @note
     *   This method is called to extract result set of a query via the connector
     *   provided and cache the result within the request.
     * 
     * @param conn
     *   a valid database connector for extracting a result set
     */
    void _setResponse(std::shared_ptr<database::mysql::Connection> const& conn);

    // Input parameters

    ProtocolRequestSql const _request;

    /// Cached result to be sent to a client
    mutable ProtocolResponseSql _response;
};

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestFS;

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestPOSIX;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERSQLREQUEST_H
