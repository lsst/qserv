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
#ifndef LSST_QSERV_REPLICA_GETDBSTATUSQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GETDBSTATUSQSERVMGTREQUEST_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/QservMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class GetDbStatusQservMgtRequest is a request for obtaining various info
 * on the database service of the Qserv worker.
 */
class GetDbStatusQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<GetDbStatusQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    GetDbStatusQservMgtRequest() = delete;
    GetDbStatusQservMgtRequest(GetDbStatusQservMgtRequest const&) = delete;
    GetDbStatusQservMgtRequest& operator=(GetDbStatusQservMgtRequest const&) = delete;

    virtual ~GetDbStatusQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param worker The name of a worker to send the request to.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<GetDbStatusQservMgtRequest> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& worker,
            CallbackType const& onFinish = nullptr);

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::notify()
    virtual void notify(replica::Lock const& lock) final;

private:
    /// @see GetDbStatusQservMgtRequest::create()
    GetDbStatusQservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                               std::string const& worker, CallbackType const& onFinish);

    // Input parameters

    CallbackType _onFinish;  ///< This callback is reset after finishing the request.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_GETDBSTATUSQSERVMGTREQUEST_H
