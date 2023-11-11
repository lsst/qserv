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
#ifndef LSST_QSERV_REPLICA_ADDREPLICAQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_ADDREPLICAQSERVMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <vector>
#include <utility>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/QservMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations

namespace lsst::qserv::replica {

/**
 * Class AddReplicaQservMgtRequest implements a request notifying Qserv workers
 * on new chunks added to the database.
 */
class AddReplicaQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<AddReplicaQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    AddReplicaQservMgtRequest() = delete;
    AddReplicaQservMgtRequest(AddReplicaQservMgtRequest const&) = delete;
    AddReplicaQservMgtRequest& operator=(AddReplicaQservMgtRequest const&) = delete;

    virtual ~AddReplicaQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param worker The name of a worker to send the request to.
     * @param chunk The chunk number.
     * @param databases The names of databases.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static Ptr create(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& worker,
                      unsigned int chunk, std::vector<std::string> const& databases,
                      CallbackType const& onFinish = nullptr);

    /// @return the chunk number
    unsigned int chunk() const { return _chunk; }

    /// @return names of databases
    std::vector<std::string> const& databases() const { return _databases; }

    /// @see QservMgtRequest::extendedPersistentState()
    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /// @see QservMgtRequest::createHttpReqImpl
    virtual void createHttpReqImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::notify
    virtual void notify(replica::Lock const& lock) final;

private:
    /// @see AddReplicaQservMgtRequest::create()
    AddReplicaQservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                              std::string const& worker, unsigned int chunk,
                              std::vector<std::string> const& databases, CallbackType const& onFinish);

    // Input parameters

    unsigned int const _chunk;
    std::vector<std::string> const _databases;
    CallbackType _onFinish;  ///< The callback is reset when the request finishes.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_ADDREPLICAQSERVMGTREQUEST_H
