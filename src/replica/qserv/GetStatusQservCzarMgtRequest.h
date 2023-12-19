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
#ifndef LSST_QSERV_REPLICA_GETSTATUSQSERVCZARMGTREQUEST_H
#define LSST_QSERV_REPLICA_GETSTATUSQSERVCZARMGTREQUEST_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/qserv/QservCzarMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class GetStatusQservCzarMgtRequest is a request for obtaining various info
 * on a status of the Qserv Czar.
 */
class GetStatusQservCzarMgtRequest : public QservCzarMgtRequest {
public:
    typedef std::shared_ptr<GetStatusQservCzarMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    GetStatusQservCzarMgtRequest() = delete;
    GetStatusQservCzarMgtRequest(GetStatusQservCzarMgtRequest const&) = delete;
    GetStatusQservCzarMgtRequest& operator=(GetStatusQservCzarMgtRequest const&) = delete;

    virtual ~GetStatusQservCzarMgtRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param czarName The name of a Czar to send the request to.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<GetStatusQservCzarMgtRequest> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& czarName,
            CallbackType const& onFinish = nullptr);

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) override;

    /// @see QservMgtRequest::notify()
    virtual void notify(replica::Lock const& lock) override;

private:
    /// @see GetStatusQservCzarMgtRequest::create()
    GetStatusQservCzarMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                 std::string const& czarName, CallbackType const& onFinish);

    // Input parameters

    CallbackType _onFinish;  ///< This callback is reset after finishing the request.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_GETSTATUSQSERVCZARMGTREQUEST_H
