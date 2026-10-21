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
#ifndef LSST_QSERV_REPLICA_HTTPFINDREQUEST_H
#define LSST_QSERV_REPLICA_HTTPFINDREQUEST_H

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

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpFindRequest represents requests to find a specific replica of a database
 * and a specific chunk of that database at a specific worker.
 * @note The replica information obtained from the request is always saved in a database.
 */
class HttpFindRequest : public HttpRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpFindRequest>)> CallbackType;

    HttpFindRequest() = delete;
    HttpFindRequest(HttpFindRequest const&) = delete;
    HttpFindRequest& operator=(HttpFindRequest const&) = delete;

    ~HttpFindRequest() final = default;

    std::string const& database() const { return _database; }
    unsigned int chunk() const { return _chunk; }
    bool computeCheckSum() const { return _computeCheckSum; }

    /**
     * @note This operation will return a sensible result only if the operation
     *  finishes with status FINISHED::SUCCESS
     * @return a reference to a result obtained from a remote service.
     */
    ReplicaInfo const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param database The name of a database.
     * @param chunk The number of a chunk to find (implies all relevant tables)
     *
     * @see The very base class RequestI for the description of the common parameters
     *  of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpFindRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& database, unsigned int chunk, CallbackType const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool computeCheckSum = false, bool keepTracking = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;
    virtual void savePersistentState(replica::Lock const& lock) final;
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) final;

private:
    HttpFindRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                    std::string const& database, unsigned int chunk, CallbackType const& onFinish,
                    int priority, bool computeCheckSum, bool keepTracking);

    // Input parameters

    std::string const _database;
    unsigned int const _chunk;
    bool const _computeCheckSum;
    CallbackType _onFinish;

    /// Result of the operation
    ReplicaInfo _replicaInfo;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPFINDREQUEST_H
