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
#ifndef LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <utility>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/qserv/QservWorkerMgtRequest.h"
#include "replica/util/ReplicaInfo.h"

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class GetReplicasQservMgtRequest implements a request retrieving a list of
 * replicas known to Qserv workers.
 */
class GetReplicasQservMgtRequest : public QservWorkerMgtRequest {
public:
    typedef std::shared_ptr<GetReplicasQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    GetReplicasQservMgtRequest() = delete;
    GetReplicasQservMgtRequest(GetReplicasQservMgtRequest const&) = delete;
    GetReplicasQservMgtRequest& operator=(GetReplicasQservMgtRequest const&) = delete;

    virtual ~GetReplicasQservMgtRequest() override = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param workerName The name of a worker to send the request to.
     * @param databaseFamily The name of a database family.
     * @param inUseOnly (optional) return replicas which are presently in use.
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                      std::string const& databaseFamily, bool inUseOnly = false,
                      CallbackType const& onFinish = nullptr);

    /// @return name of a database family
    std::string const& databaseFamily() const { return _databaseFamily; }

    /// @return flag indicating (if set) to report a subset of chunks which are in use
    bool inUseOnly() const { return _inUseOnly; }

    /**
     * @return A collection of replicas reported from the corresponding Qserv worker.
     * @throw std::logic_error If called before the request finishes or if it's finished with
     *   any status but SUCCESS.
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
    /// @see GetReplicasQservMgtRequest::create()
    GetReplicasQservMgtRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                               std::string const& databaseFamily, bool inUseOnly,
                               CallbackType const& onFinish);

    // Input parameters

    std::string const _databaseFamily;
    bool const _inUseOnly;
    CallbackType _onFinish;  ///< The callback function is reset when the request finishes.

    /// A collection of replicas reported by the Qserr worker
    QservReplicaCollection _replicas;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_GET_REPLICAS_QSERVMGTREQUEST_H
