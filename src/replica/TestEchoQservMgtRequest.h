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
#ifndef LSST_QSERV_REPLICA_TESTECHOQSERVMGTREQUEST_H
#define LSST_QSERV_REPLICA_TESTECHOQSERVMGTREQUEST_H

// System headers
#include <list>
#include <memory>
#include <string>
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
 * Class TestEchoQservMgtRequest a special kind of requests
 * for testing Qserv workers.
 */
class TestEchoQservMgtRequest : public QservMgtRequest {
public:
    typedef std::shared_ptr<TestEchoQservMgtRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    TestEchoQservMgtRequest() = delete;
    TestEchoQservMgtRequest(TestEchoQservMgtRequest const&) = delete;
    TestEchoQservMgtRequest& operator=(TestEchoQservMgtRequest const&) = delete;

    virtual ~TestEchoQservMgtRequest() final = default;

    /**
     * Static factory method is needed to prevent issues with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * @param serviceProvider A reference to a provider of services for accessing
     *   Configuration, saving the request's persistent state to the database.
     * @param worker The name of a worker to send the request to.
     * @param data The data string to be echoed back by the worker (if successful).
     * @param onFinish (optional) callback function to be called upon request completion.
     * @return A pointer to the created object.
     */
    static std::shared_ptr<TestEchoQservMgtRequest> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& worker,
            std::string const& data, CallbackType const& onFinish = nullptr);

    /// @return input data string sent to the worker
    std::string const& data() const { return _data; }

    /**
     * @return The data string echoed back by the worker.
     * @note The method will throw exception std::logic_error if called before
     *   the request finishes or if it's finished with any status but SUCCESS.
     */
    std::string const& dataEcho() const;

    /// @see QservMgtRequest::extendedPersistentState()
    virtual std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

protected:
    /// @see QservMgtRequest::createHttpReqImpl()
    virtual void createHttpReqImpl(replica::Lock const& lock) final;

    /// @see QservMgtRequest::dataReady()
    virtual QservMgtRequest::ExtendedState dataReady(replica::Lock const& lock,
                                                     nlohmann::json const& data) final;

    /// @see QservMgtRequest::notify()
    virtual void notify(replica::Lock const& lock) final;

private:
    /// @see TestEchoQservMgtRequest::create()
    TestEchoQservMgtRequest(std::shared_ptr<ServiceProvider> const& serviceProvider,
                            std::string const& worker, std::string const& data, CallbackType const& onFinish);

    // Input parameters

    std::string const _data;
    CallbackType _onFinish;  ///< This callback is reset after finishing the request.

    /// The data string returned by the Qserv worker
    std::string _dataEcho;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_TESTECHOQSERVMGTREQUEST_H
