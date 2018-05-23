/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/// SetReplicasQservMgtRequest.h declares:
///
/// class SetReplicasQservMgtRequest
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>
#include <vector>

// Third party headers

// Qserv headers
#include "replica/QservMgtRequest.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"
#include "wpublish/SetChunkListQservRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class SetReplicasQservMgtRequest implements a request for setting new
  * collections Qserv workers.
  */
class SetReplicasQservMgtRequest
    :   public QservMgtRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SetReplicasQservMgtRequest> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SetReplicasQservMgtRequest() = delete;
    SetReplicasQservMgtRequest(SetReplicasQservMgtRequest const&) = delete;
    SetReplicasQservMgtRequest& operator=(SetReplicasQservMgtRequest const&) = delete;

    ~SetReplicasQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - reference to a provider of services
     * @param io_service      - BOOST ASIO service
     * @param worker          - name of a worker
     * @param newReplicas     - collection of new replicas (NOTE: useCount field is ignored)
     * @param force           - proceed with the operation even if some replicas affceted by
     *                          the operation are in use.
     * @param onFinish        - callback function to be called upon request completion
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      QservReplicaCollection const& newReplicas,
                      bool force = false,
                      CallbackType onFinish = nullptr);

    /// @return collection of new replicas to be set at the Qserv worker
    QservReplicaCollection const& newReplicas() const { return _newReplicas; }

    /// @return flag indicating (if set) the 'force' mode of the operation
    bool force() const { return _force; }

    /**
      * @return previous collection of replicas which was set at the corresponding
      *         Qserv worker before the operation.
      *
      * ATTENTION: the method will throw exception std::logic_error if called
      * before the request finishes or if it's finished with any but SUCCESS
      * status.
      */
    QservReplicaCollection const& replicas() const;

    /**
     * Implement the corresponding method of the base class.
     *
     * @see QservMgtRequest::extendedPersistentState()
     */
     std::string extendedPersistentState(SqlGeneratorPtr const& gen) const override;

private:

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @param serviceProvider - reference to a provider of services
     * @param io_service      - BOOST ASIO service
     * @param worker          - the name of a worker
     * @param newReplicas     - collection of new replicas (NOTE: useCount field is ignored)
     * @param bool            - proceed with the operation even if some replicas affceted by
     *                          the operation are in use.
     * @param onFinish        - callback function to be called upon request completion
     */
    SetReplicasQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                               boost::asio::io_service& io_service,
                               std::string const& worker,
                               QservReplicaCollection const& newReplicas,
                               bool force,
                               CallbackType onFinish);

    /**
     * Carry over results of the request into a local collection.
     * @param collection - input collection of replicas
     */
     void setReplicas(wpublish::SetChunkListQservRequest::ChunkCollection const& collection);

    /**
      * Implement the corresponding method of the base class
      *
      * @see QservMgtRequest::startImpl
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * Implement the corresponding method of the base class
      *
      * @see QservMgtRequest::finishImpl
      */
    void finishImpl(util::Lock const& lock) final;

    /**
      * Implement the corresponding method of the base class
      *
      * @see QservMgtRequest::notifyImpl
      */
    void notifyImpl() final;

private:

    /// A collection of new replicas to be set at the Qserv worker
    QservReplicaCollection _newReplicas;

    /// Flag indicating to report (if set) the 'force' mode of the operation
    bool _force;

    /// The callback function for sending a notification upon request completion
    CallbackType _onFinish;

    /// A request to the remote services
    wpublish::SetChunkListQservRequest::Ptr _qservRequest;

    /// A collection of replicas reported by the Qservr worker
    QservReplicaCollection _replicas;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SETREPLICASQSERVMGTREQUEST_H
