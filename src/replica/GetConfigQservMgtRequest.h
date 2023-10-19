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
#ifndef LSST_QSERV_REPLICA_GETCONFIGQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GETCONFIGQSERVMGTREQUEST_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/QservMgtRequest.h"
#include "replica/ServiceProvider.h"
#include "xrdreq/GetConfigQservRequest.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class GetConfigQservMgtRequest is a request for obtaining configuration
 * parameters of the Qserv worker.
 */
class GetConfigQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<GetConfigQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    GetConfigQservMgtRequest() = delete;
    GetConfigQservMgtRequest(GetConfigQservMgtRequest const&) = delete;
    GetConfigQservMgtRequest& operator=(GetConfigQservMgtRequest const&) = delete;

    virtual ~GetConfigQservMgtRequest() = default;

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
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      CallbackType const& onFinish = nullptr);

    /**
     * @return The info object returned back by the worker.
     * @note The method will throw exception std::logic_error if called before
     *   the request finishes or if it's finished with any status but SUCCESS.
     */
    nlohmann::json const& info() const;

protected:
    /// @see QservMgtRequest::startImpl()
    virtual void startImpl(replica::Lock const& lock);

    /// @see QservMgtRequest::finishImpl()
    virtual void finishImpl(replica::Lock const& lock);

    /// @see QservMgtRequest::notify()
    virtual void notify(replica::Lock const& lock);

private:
    /// @see GetConfigQservMgtRequest::create()
    GetConfigQservMgtRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                             CallbackType const& onFinish);

    /**
     * Carry over results of the request into a local storage.
     * @param lock A lock on QservMgtRequest::_mtx must be acquired by a caller of the method.
     * @param info The data string returned by a worker.
     */
    void _setInfo(replica::Lock const& lock, std::string const& info);

    // Input parameters

    std::string const _data;
    CallbackType _onFinish;  ///< this object is reset after finishing the request

    /// A request to the remote services
    xrdreq::GetConfigQservRequest::Ptr _qservRequest;

    /// The info object returned by the Qserv worker
    nlohmann::json _info;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_GETCONFIGQSERVMGTREQUEST_H
