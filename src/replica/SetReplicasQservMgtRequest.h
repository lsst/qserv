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
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/QservMgtRequest.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"
#include "wpublish/SetChunkListQservRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SetReplicasQservMgtRequest implements a request for setting new
 * collections Qserv workers.
 */
class SetReplicasQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<SetReplicasQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    SetReplicasQservMgtRequest() = delete;
    SetReplicasQservMgtRequest(SetReplicasQservMgtRequest const&) = delete;
    SetReplicasQservMgtRequest& operator=(SetReplicasQservMgtRequest const&) = delete;

    ~SetReplicasQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param worker The name of a worker to send the request to.
     * @param newReplicas A collection of new replicas (NOTE: useCount field is ignored).
     * @param databases Limit a scope of the operation to databases of this collection.
     * @param force Proceed with the operation even if some replicas affected by
     *   the operation are in use.
     * @param onFinish A callback function to be called upon request completion.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      QservReplicaCollection const& newReplicas, std::vector<std::string> const& databases,
                      bool force = false, CallbackType const& onFinish = nullptr);

    /// @return collection of new replicas to be set at the Qserv worker
    QservReplicaCollection const& newReplicas() const { return _newReplicas; }

    /// @return flag indicating (if set) the 'force' mode of the operation
    bool force() const { return _force; }

    /**
     * @return A previous collection of replicas which was set at the corresponding
     *   Qserv worker before the operation.
     * @note The method will throw exception std::logic_error if called before
     *    the request finishes or if it's finished with any but SUCCESS status.
     */
    QservReplicaCollection const& replicas() const;

    /// @see QservMgtRequest::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /// @see QservMgtRequest::startImpl
    void startImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::finishImpl
    void finishImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::notify
    void notify(replica::Lock const& lock) final;

private:
    /// @see SetReplicasQservMgtRequest::create()
    SetReplicasQservMgtRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                               QservReplicaCollection const& newReplicas,
                               std::vector<std::string> const& databases, bool force,
                               CallbackType const& onFinish);

    /**
     * Carry over results of the request into a local collection.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired before calling this method.
     * @param collection The input collection of replicas.
     */
    void _setReplicas(replica::Lock const& lock,
                      wpublish::SetChunkListQservRequest::ChunkCollection const& collection);

    // Input parameters.

    QservReplicaCollection const _newReplicas;
    std::vector<std::string> const _databases;

    bool const _force;
    CallbackType _onFinish;

    /// A request to the remote services.
    wpublish::SetChunkListQservRequest::Ptr _qservRequest;

    /// A collection of replicas reported by the Qserv worker.
    QservReplicaCollection _replicas;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SETREPLICASQSERVMGTREQUEST_H
