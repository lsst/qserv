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
#ifndef LSST_QSERV_HTTPREPLICATIONLEVELSMODULE_H
#define LSST_QSERV_HTTPREPLICATIONLEVELSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HealthMonitorTask.h"
#include "replica/HttpModule.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpReplicationLevelsModule implements a handler for the replication
 * levels requests.
 */
class HttpReplicationLevelsModule: public HttpModule {
public:

    typedef std::shared_ptr<HttpReplicationLevelsModule> Ptr;

    static Ptr create(Controller::Ptr const& controller,
                      std::string const& taskName,
                      unsigned int workerResponseTimeoutSec,
                      HealthMonitorTask::Ptr const& healthMonitorTask);

    HttpReplicationLevelsModule() = delete;
    HttpReplicationLevelsModule(HttpReplicationLevelsModule const&) = delete;
    HttpReplicationLevelsModule& operator=(HttpReplicationLevelsModule const&) = delete;

    ~HttpReplicationLevelsModule() final = default;

protected:

    void executeImpl(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp,
                     std::string const& subModuleName) final;

private:

    HttpReplicationLevelsModule(Controller::Ptr const& controller,
                                std::string const& taskName,
                                unsigned int workerResponseTimeoutSec,
                                HealthMonitorTask::Ptr const& healthMonitorTask);

    // Input parameters

    /// A reference to the smart pointer is used to avoid increasing
    /// the reference counter to the pointed object and to avoid the circular
    /// dependency which would prevent object destruction.
    /// TODO: consider the weak pointer?
    HealthMonitorTask::Ptr const& _healthMonitorTask;

    /// The cached state of the last replication levels report
    nlohmann::json _replicationLevelReport = nlohmann::json::object();

    /// The time of the last cached report
    uint64_t _replicationLevelReportTimeMs = 0;

    /// Protects the replication level cache
    util::Mutex _replicationLevelMtx;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPREPLICATIONLEVELSMODULE_H
