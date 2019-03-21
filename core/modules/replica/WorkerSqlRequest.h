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

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerSqlRequest executes queries against the worker database
 * and return results sets (if any) back to a caller.
 *
 * @note
 *   queries passed into this operation must be well formed.
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
     * @param priority
     *   indicates the importance of the request
     *
     * @param query
     *   the query to be executed
     *
     * @param user
     *   the name of a database account for connecting to the database service
     *
     * @param password
     *   a database for connecting to the database service
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password);

    // Default construction and copy semantics are prohibited

    WorkerSqlRequest() = delete;
    WorkerSqlRequest(WorkerSqlRequest const&) = delete;
    WorkerSqlRequest& operator=(WorkerSqlRequest const&) = delete;

    ~WorkerSqlRequest() override = default;

    // Trivial get methods

    std::string const& query()    const { return _query; }
    std::string const& user()     const { return _user; }
    std::string const& password() const { return _password; }
    
    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseSql& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

protected:

    /// @see WorkerSqlRequest::create()
    WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password);

    // Input parameters

    std::string const _query;
    std::string const _user;
    std::string const _password;
};

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestFS;

/// Class WorkerSqlRequest provides an actual implementation
typedef WorkerSqlRequest WorkerSqlRequestPOSIX;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERSQLREQUEST_H
