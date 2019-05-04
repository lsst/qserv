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
#ifndef LSST_QSERV_REPLICA_GETSTATUSQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_GETSTATUSQSERVMGTREQUEST_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/QservMgtRequest.h"
#include "replica/ServiceProvider.h"
#include "wpublish/GetStatusQservRequest.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class GetStatusQservMgtRequest is a request for obtaining various info
 * (status, counters, monitoring) reported the Qserv workers.
 */
class GetStatusQservMgtRequest : public QservMgtRequest {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<GetStatusQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    GetStatusQservMgtRequest() = delete;
    GetStatusQservMgtRequest(GetStatusQservMgtRequest const&) = delete;
    GetStatusQservMgtRequest& operator=(GetStatusQservMgtRequest const&) = delete;

    ~GetStatusQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   reference to a provider of services for accessing Configuration,
     *   saving the request's persistent state to the database
     *
     * @param worker
     *   the name of a worker to send the request to
     *
     * @param onFinish
     *   (optional) callback function to be called upon request completion
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      CallbackType const& onFinish=nullptr);

    /**
     * @return
     *   info object returned back by the worker
     *
     * @note
     *   the method will throw exception std::logic_error if called
     *   before the request finishes or if it's finished with any
     *   status but SUCCESS.
     */
    nlohmann::json const& info() const;

    /// @see QservMgtRequest::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

protected:

    void startImpl(util::Lock const& lock) final;

    void finishImpl(util::Lock const& lock) final;

    void notify(util::Lock const& lock) final;

private:

    /// @see GetStatusQservMgtRequest::created()
    GetStatusQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                             std::string const& worker,
                             CallbackType const& onFinish);

    /**
     * Carry over results of the request into a local storage.
     * 
     * @param lock
     *   lock on QservMgtRequest::_mtx must be acquired by a caller of the method
     *
     * @param info
     *   data string returned by a worker
     */
    void _setInfo(util::Lock const& lock,
                  std::string const& info);


    // Input parameters

    std::string const _data;
    CallbackType      _onFinish;    /// @note this object is reset after finishing the request

    /// A request to the remote services
    wpublish::GetStatusQservRequest::Ptr _qservRequest;

    /// The info object returned by the Qserv worker
    nlohmann::json _info;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_GETSTATUSQSERVMGTREQUEST_H
