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
#include "replica/HttpProcessorConfig.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpProcessor processes requests from the built-in HTTP server.
 * The constructor of the class will register modules to which the incoming
 * requests will be forwarded for handling.
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
                      HttpProcessorConfig const& processorConfig,
                      HealthMonitorTask::Ptr const& healthMonitorTask);

private:
    HttpProcessor(Controller::Ptr const& controller,
                  HttpProcessorConfig const& processorConfig,
                  HealthMonitorTask::Ptr const& healthMonitorTask);

    void _initialize();

    // Input parameters

    HttpProcessorConfig const _processorConfig;
    HealthMonitorTask::Ptr const _healthMonitorTask;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPPROCESSOR_H
