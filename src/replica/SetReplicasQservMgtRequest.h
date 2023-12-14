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
#ifndef LSST_QSERV_REPLICA_SETREPLICASQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_SETREPLICASQSERVMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <vector>
#include <utility>

// Qserv headers
#include "replica/QservWorkerMgtRequest.h"
#include "replica/ReplicaInfo.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SetReplicasQservMgtRequest implements a request for configuring chunk
 * inventory=ies (both transient and persistent) at Qserv workers.
 */
class SetReplicasQservMgtRequest : public QservWorkerMgtRequest {
public:
    typedef std::shared_ptr<SetReplicasQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SetReplicasQservMgtRequest() = delete;
    SetReplicasQservMgtRequest(SetReplicasQservMgtRequest const&) = delete;
    SetReplicasQservMgtRequest& operator=(SetReplicasQservMgtRequest const&) = delete;

    virtual ~SetReplicasQservMgtRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @note A collection of replicas passed into the request will be sanitized
     *   to leave a subset of replicas belonging to databases specified in
     *   the parameter 'databases' before this request is sent to the worker.
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param workerName The name of a worker to send the request to.
     * @param newReplicas A collection of new replicas (NOTE: useCount field is ignored).
     * @param databases A set of databases that defines a scope of a scope of the request.
     * @param force Proceed with the operation even if some replicas affected by
     *   the operation are in use.
     * @param onFinish A callback function to be called upon request completion.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                      QservReplicaCollection const& newReplicas, std::vector<std::string> const& databases,
                      bool force = false, CallbackType const& onFinish = nullptr);

    /// @return a collection of replicas passed into the request
    QservReplicaCollection const& newReplicas() const { return _newReplicas; }

    /// @return a flag indicating (if set) the 'force' mode of the operation
    bool force() const { return _force; }

    /**
     * @return The previous collection of replicas that was configured at Qserv worker
     *   at a time when the request was executed. The collection will only include
     *   replicas belonging to databases mentioned in the parameter 'databases'
     *   of the request.
     * @throw std::logic_error if called before the request finishes or if it's finished
     *   with any but SUCCESS status.
     */
    QservReplicaCollection const& replicas() const;

    /// @see QservMgtRequest::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) override;

    /// @see QservMgtRequest::dataReady()
    virtual QservMgtRequest::ExtendedState dataReady(replica::Lock const& lock,
                                                     nlohmann::json const& data) override;

    /// @see QservMgtRequest::notify
    virtual void notify(replica::Lock const& lock) override;

private:
    /// @see SetReplicasQservMgtRequest::create()
    SetReplicasQservMgtRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                               QservReplicaCollection const& newReplicas,
                               std::vector<std::string> const& databases, bool force,
                               CallbackType const& onFinish);

    // Input parameters.

    QservReplicaCollection const _newReplicas;
    std::vector<std::string> const _databases;
    bool const _force;
    CallbackType _onFinish;  ///< The callback function is reset when the request finishes.

    /// A collection of replicas reported by the Qserv worker.
    QservReplicaCollection _replicas;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SETREPLICASQSERVMGTREQUEST_H
