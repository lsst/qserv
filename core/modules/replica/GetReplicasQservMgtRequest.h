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
#ifndef LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H

/// GetReplicasQservMgtRequest.h declares:
///
/// class GetReplicasQservMgtRequest
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
#include "wpublish/GetChunkListQservRequest.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class GetReplicasQservMgtRequest implements a request retreiving a list of
  * replicas known to Qserv workers.
  */
class GetReplicasQservMgtRequest
    :   public QservMgtRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<GetReplicasQservMgtRequest> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    GetReplicasQservMgtRequest() = delete;
    GetReplicasQservMgtRequest(GetReplicasQservMgtRequest const&) = delete;
    GetReplicasQservMgtRequest& operator=(GetReplicasQservMgtRequest const&) = delete;

    ~GetReplicasQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - reference to a provider of services
     * @param io_service      - BOOST ASIO service
     * @param worker          - the name of a worker
     * @param databaseFamily  - the name of a database family
     * @param inUseOnly       - (optional) return replicas which're presently in use
     * @param onFinish        - (optional) callback function to be called upon request completion
     * 
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& databaseFamily,
                      bool inUseOnly = false,
                      CallbackType onFinish = nullptr);

    /// @return name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return flag indicating (if set) to report a subset of chunks which are in use
    bool inUseOnly() const { return _inUseOnly; }

    /**
     * @return collection of replicas repored from the corresponding Qserv worker
     *
     * ATTENTION: the method will throw exception std::logic_error if called
     * before th erequest finishes or if it's finished with any but SUCCESS
     * status.
     */
    QservReplicaCollection const& replicas() const;

    /**
     * @see QservMgtRequest::extendedPersistentState()
     */
     std::string extendedPersistentState(SqlGeneratorPtr const& gen) const override;

private:

    /**
     * Construct the request with the pointer to the services provider
     *
     * @see GetReplicasQservMgtRequest::created()
     */
    GetReplicasQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                               boost::asio::io_service& io_service,
                               std::string const& worker,
                               std::string const& databaseFamily,
                               bool inUseOnly,
                               CallbackType onFinish);

    /**
     * Carry over results of the request into a local collection. Filter results
     * by databases participating in the family.
     * 
     * @param collection - input collection of replicas
     */
     void setReplicas(wpublish::GetChunkListQservRequest::ChunkCollection const& collection);

    /**
      * @see QservMgtRequest::startImpl
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * @see QservMgtRequest::finishImpl
      */
    void finishImpl(util::Lock const& lock) final;

    /**
      * @see QservMgtRequest::notifyImpl
      */
    void notifyImpl() final;

private:

    /// The name of a database family
    std::string _databaseFamily;

    /// Flag indicating to report (if set) a subset of chunks which are in use
    bool _inUseOnly;

    /// The callback function for sending a notification upon request completion
    CallbackType _onFinish;

    /// A request to the remote services
    wpublish::GetChunkListQservRequest::Ptr _qservRequest;

    /// A collection of replicas reported by the Qservr worker
    QservReplicaCollection _replicas;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H
