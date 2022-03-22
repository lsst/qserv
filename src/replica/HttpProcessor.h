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
#include <memory>
#include <string>

// Qserv headers
#include "replica/Controller.h"
#include "replica/EventLogger.h"
#include "replica/HealthMonitorTask.h"
#include "replica/HttpProcessorConfig.h"
#include "replica/HttpSvc.h"
#include "replica/NamedMutexRegistry.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpProcessor processes requests from the built-in HTTP server.
 * 
 * @note The class's implementation starts its own collection of BOOST ASIO
 *   service threads as configured in Configuration.
 */
class HttpProcessor: public HttpSvc, public EventLogger {
public:
    typedef std::shared_ptr<HttpProcessor> Ptr;

    HttpProcessor() = delete;
    HttpProcessor(HttpProcessor const&) = delete;
    HttpProcessor& operator=(HttpProcessor const&) = delete;

    /// Non-trivial destructor is needed for a purpose of logging the shutdown event
    virtual ~HttpProcessor();

    /**
     * Create an instance of the service.
     *
     * @param controller For configuration, etc. services.
     * @param processorConfig Configuration parameters of the processor.
     * @return A pointer to the created object.
     */
    static Ptr create(Controller::Ptr const& controller,
                      HttpProcessorConfig const& processorConfig,
                      HealthMonitorTask::Ptr const& healthMonitorTask);

protected:
    /// @see HttpSvc::context()
    virtual std::string const& context() const;

    /// @see HttpSvc::registerServices()
    virtual void registerServices();

private:
    /// @see HttpProcessor::create()
    HttpProcessor(Controller::Ptr const& controller,
                  HttpProcessorConfig const& processorConfig,
                  HealthMonitorTask::Ptr const& healthMonitorTask);

    // Input parameters

    HttpProcessorConfig const _processorConfig;
    HealthMonitorTask::Ptr const _healthMonitorTask;

    /// Named mutexes are used for acquiring exclusive transient locks on the transaction
    /// management operations performed by the relevant modules.
    NamedMutexRegistry _transactionMutexRegistry;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPPROCESSOR_H
