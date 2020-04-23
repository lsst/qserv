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
#ifndef LSST_QSERV_HTTPWORKERSTATUSMODULE_H
#define LSST_QSERV_HTTPWORKERSTATUSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HealthMonitorTask.h"
#include "replica/HttpModule.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpWorkerStatusModule implements a handler for the worker
 * status requests.
 */
class HttpWorkerStatusModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpWorkerStatusModule> Ptr;

    static void process(Controller::Ptr const& controller,
                        std::string const& taskName,
                        HttpProcessorConfig const& processorConfig,
                        qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp,
                        HealthMonitorTask::Ptr const& healthMonitorTask,
                        std::string const& subModuleName=std::string(),
                        HttpModule::AuthType const authType=HttpModule::AUTH_NONE);

    HttpWorkerStatusModule() = delete;
    HttpWorkerStatusModule(HttpWorkerStatusModule const&) = delete;
    HttpWorkerStatusModule& operator=(HttpWorkerStatusModule const&) = delete;

    ~HttpWorkerStatusModule() final = default;

protected:
    void executeImpl(std::string const& subModuleName) final;

private:
    HttpWorkerStatusModule(Controller::Ptr const& controller,
                           std::string const& taskName,
                           HttpProcessorConfig const& processorConfig,
                           qhttp::Request::Ptr const& req,
                           qhttp::Response::Ptr const& resp,
                           HealthMonitorTask::Ptr const& healthMonitorTask);

    // Input parameters

    std::weak_ptr<HealthMonitorTask> const _healthMonitorTask;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPWORKERSTATUSMODULE_H
