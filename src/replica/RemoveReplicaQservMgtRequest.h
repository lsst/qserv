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
#ifndef LSST_QSERV_REPLICA_REMOVEREPLICAQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_REMOVEREPLICAQSERVMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <vector>
#include <utility>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/QservWorkerMgtRequest.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class RemoveReplicaQservMgtRequest implements a request notifying Qserv workers
 * on new chunks added to the database.
 */
class RemoveReplicaQservMgtRequest : public QservWorkerMgtRequest {
public:
    typedef std::shared_ptr<RemoveReplicaQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    RemoveReplicaQservMgtRequest() = delete;
    RemoveReplicaQservMgtRequest(RemoveReplicaQservMgtRequest const&) = delete;
    RemoveReplicaQservMgtRequest& operator=(RemoveReplicaQservMgtRequest const&) = delete;

    virtual ~RemoveReplicaQservMgtRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param workerName The name of a worker to send the request to.
     * @param chunk The chunk whose replicas will be disabled at the Qserv worker.
     * @param databases The names of databases.
     * @param force Force the removal even if the chunk is in use.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static Ptr create(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName,
                      unsigned int chunk, std::vector<std::string> const& databases, bool force = false,
                      CallbackType const& onFinish = nullptr);

    /// @return number of a chunk
    unsigned int chunk() const { return _chunk; }

    /// @return names of databases
    std::vector<std::string> const& databases() const { return _databases; }

    /// @return flag indicating of the chunk removal should be forced even if in use
    bool force() const { return _force; }

    /// @see QservMgtRequest::extendedPersistentState()
    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    /// @see QservMgtRequest::createHttpReqImpl
    virtual void createHttpReqImpl(replica::Lock const& lock) override;

    /// @see QservMgtRequest::notify
    virtual void notify(replica::Lock const& lock) override;

private:
    /// @see RemoveReplicaQservMgtRequest::create()
    RemoveReplicaQservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                 std::string const& workerName, unsigned int chunk,
                                 std::vector<std::string> const& databases, bool force,
                                 CallbackType const& onFinish);

    // Input parameters

    unsigned int const _chunk;
    std::vector<std::string> const _databases;
    bool const _force;
    CallbackType _onFinish;  ///< The callback is reset when the request finishes.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REMOVEREPLICAQSERVMGTREQUEST_H
