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
#ifndef LSST_QSERV_REPLICA_HTTPCREATEREPLICAREQUEST_H
#define LSST_QSERV_REPLICA_HTTPCREATEREPLICAREQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/requests/http/HttpRequest.h"
#include "replica/util/Common.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::http {
class AsyncReq;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpCreateReplicaRequest represents a transient state of requests
 * within the master controller for creating replicas.
 */
class HttpCreateReplicaRequest : public HttpRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpCreateReplicaRequest>)> CallbackType;

    HttpCreateReplicaRequest() = delete;
    HttpCreateReplicaRequest(HttpCreateReplicaRequest const&) = delete;
    HttpCreateReplicaRequest& operator=(HttpCreateReplicaRequest const&) = delete;

    virtual ~HttpCreateReplicaRequest() final = default;

    std::string const& database() const { return _database; }
    unsigned int chunk() const { return _chunk; }
    std::string const& sourceWorkerName() const { return _sourceWorkerName; }

    /**
     * @return request-specific extended data reported upon a successful
     *  completion of the request
     */
    ReplicaInfo const& responseData() const { return _replicaInfo; }

    /**
     * Create and start a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param sourceWorkerName An identifier of a worker node at a source of the chunk.
     * @param database The name of a database.
     * @param chunk The number of a chunk to replicate (implies all relevant tables).
     *
     * @see The very base class RequestI for the description of the common parameters
     *  of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpCreateReplicaRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& sourceWorkerName, std::string const& database, unsigned int chunk,
            CallbackType const& onFinish = nullptr, int priority = PRIORITY_NORMAL, bool keepTracking = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;
    virtual void savePersistentState(replica::Lock const& lock) final;
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) final;

private:
    HttpCreateReplicaRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                             std::string const& sourceWorkerName, std::string const& database,
                             unsigned int chunk, CallbackType const& onFinish, int priority,
                             bool keepTracking);

    // Input parameters

    std::string const _database;
    unsigned int const _chunk;
    std::string const _sourceWorkerName;
    CallbackType _onFinish;  ///< @note reset when the request is finished

    /// Detailed info on the replica status
    ReplicaInfo _replicaInfo;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPCREATEREPLICAREQUEST_H
