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
#ifndef LSST_QSERV_REPLICA_HTTPSQLCREATEDBREQUEST_H
#define LSST_QSERV_REPLICA_HTTPSQLCREATEDBREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/requests/http/HttpSqlRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpSqlCreateDbRequest represents Controller-side requests for initiating
 * queries for creating databases at a remote worker nodes.
 */
class HttpSqlCreateDbRequest : public HttpSqlRequest {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<HttpSqlCreateDbRequest>)> CallbackType;

    /**
     * Create a new request with specified parameters.
     *
     * Class-specific parameters are documented below:
     * @param database The name of a database to be created.
     * @see The very base class for the description of the common parameters of all subclasses.
     * @return A smart pointer to the created object.
     */
    static std::shared_ptr<HttpSqlCreateDbRequest> createAndStart(
            std::shared_ptr<Controller> const& controller, std::string const& workerName,
            std::string const& database, CallbackType const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    HttpSqlCreateDbRequest() = delete;
    HttpSqlCreateDbRequest(HttpSqlCreateDbRequest const&) = delete;
    HttpSqlCreateDbRequest& operator=(HttpSqlCreateDbRequest const&) = delete;

    virtual ~HttpSqlCreateDbRequest() final = default;

protected:
    virtual void notify(replica::Lock const& lock) final;

private:
    HttpSqlCreateDbRequest(std::shared_ptr<Controller> const& controller, std::string const& workerName,
                           std::string const& database, CallbackType const& onFinish, int priority,
                           bool keepTracking);

    CallbackType _onFinish;  ///< @note is reset when the request finishes
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_HTTPSQLCREATEDBREQUEST_H
