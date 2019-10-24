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
#ifndef LSST_QSERV_HTTPPROCESSOR_H
#define LSST_QSERV_HTTPPROCESSOR_H

// System headers
#include <functional>
#include <memory>

// Qserv headers
#include "replica/EventLogger.h"
#include "replica/HealthMonitorTask.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class HttpModule;
}}} // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpProcessor processes requests from the built-in HTTP server.
 * The constructor of the class will register requests handlers an start
 * the server. Request handlers are provided by various modules instantiated
 * by this class.
 */
class HttpProcessor:
        public EventLogger,
        public std::enable_shared_from_this<HttpProcessor> {
public:

    typedef std::shared_ptr<HttpProcessor> Ptr;

    HttpProcessor() = delete;
    HttpProcessor(HttpProcessor const&) = delete;
    HttpProcessor& operator=(HttpProcessor const&) = delete;

    ~HttpProcessor();

    static Ptr create(Controller::Ptr const& controller,
                      unsigned int workerResponseTimeoutSec,
                      HealthMonitorTask::Ptr const& healthMonitorTask);

private:

    HttpProcessor(Controller::Ptr const& controller,
                  unsigned int workerResponseTimeoutSec,
                  HealthMonitorTask::Ptr const& healthMonitorTask);

    void _initialize();

    std::shared_ptr<HttpModule> const _catalogsModule;
    std::shared_ptr<HttpModule> const _replicationLevelsModule;
    std::shared_ptr<HttpModule> const _workerStatusModule;
    std::shared_ptr<HttpModule> const _controllersModule;
    std::shared_ptr<HttpModule> const _requestsModule;
    std::shared_ptr<HttpModule> const _jobsModule;
    std::shared_ptr<HttpModule> const _configurationModule;
    std::shared_ptr<HttpModule> const _qservMonitorModule;
    std::shared_ptr<HttpModule> const _qservSqlModule;
    std::shared_ptr<HttpModule> const _ingestModule;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPPROCESSOR_H
