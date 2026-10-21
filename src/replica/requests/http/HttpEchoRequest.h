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
#ifndef LSST_QSERV_REPLICA_HTTPECHOREQUEST_H
#define LSST_QSERV_REPLICA_HTTPECHOREQUEST_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/requests/http/HttpRequest.h"
#include "replica/util/Common.h"

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
 * Class HttpEchoRequest represents Controller-side requests for testing
 * the controller-worker protocol and the worker-side framework.
 * These requests have no side effects.
 */
class HttpEchoRequest : public HttpRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpEchoRequest>)> CallbackType;

    HttpEchoRequest() = delete;
    HttpEchoRequest(HttpEchoRequest const&) = delete;
    HttpEchoRequest& operator=(HttpEchoRequest const&) = delete;

    virtual ~HttpEchoRequest() final = default;

    std::string const& data() const { return _data; }
    std::uint64_t delay() const { return _delay; }

    /**
     * @note This operation will return a sensible result only if the operation
     *  finishes with status FINISHED::SUCCESS
     * @return a reference to a result obtained from a remote service.
     */
    std::string const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * Class-specific parameters are documented below:
     * @param data The data string to be echoed back by a worker.
     * @param delay The execution time (milliseconds) of the request at worker.
     *
     * @see The very base class Request for the description of the common parameters
     *  of all subclasses.
     *
     * @return A pointer to the created object.
     */
    static std::shared_ptr<HttpEchoRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& data, uint64_t delay, CallbackType const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;
    virtual void savePersistentState(replica::Lock const& lock) final;
    virtual void analyze(std::shared_ptr<http::AsyncReq> req, std::string error) final;

private:
    HttpEchoRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                    std::string const& data, uint64_t delay, CallbackType const& onFinish, int priority,
                    bool keepTracking);

    // Input parameters

    std::string const _data;
    std::uint64_t const _delay;
    CallbackType _onFinish;  ///< @note is reset when the request finishes

    /// The results reported by a worker service
    std::string _responseData;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPECHOREQUEST_H
