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
#ifndef LSST_QSERV_REPLICA_POSTEVENTQSERVCZARMGTREQUEST_H
#define LSST_QSERV_REPLICA_POSTEVENTQSERVCZARMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <utility>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "cconfig/DataManagementEvent.h"
#include "replica/qserv/QservCzarMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class PostEventQservCzarMgtRequest implements a request for posting the data
 * management events to the Qserv Czar.
 */
class PostEventQservCzarMgtRequest : public QservCzarMgtRequest {
public:
    typedef std::shared_ptr<PostEventQservCzarMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    PostEventQservCzarMgtRequest() = delete;
    PostEventQservCzarMgtRequest(PostEventQservCzarMgtRequest const&) = delete;
    PostEventQservCzarMgtRequest& operator=(PostEventQservCzarMgtRequest const&) = delete;

    virtual ~PostEventQservCzarMgtRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param czarName The name of a Czar to send the request to.
     * @param event The management event to be posted to the Czar.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& czarName,
                      cconfig::DataManagementEvent const& event, CallbackType const& onFinish = nullptr);

    /// @see QservMgtRequest::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) override;

    /// @see QservMgtRequest::notify
    virtual void notify(replica::Lock const& lock) override;

private:
    /// @see PostEventQservCzarMgtRequest::create()
    PostEventQservCzarMgtRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& czarName,
                                 cconfig::DataManagementEvent const& event, CallbackType const& onFinish);

    // Input parameters

    cconfig::DataManagementEvent const _event;  ///< The management event to be posted to the Czar.
    CallbackType _onFinish;                     ///< The callback function is reset when the request finishes.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_POSTEVENTQSERVCZARMGTREQUEST_H
