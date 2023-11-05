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
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpReplicationLevelsModule implements a handler for the replication
 * levels requests.
 */
class HttpReplicationLevelsModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpReplicationLevelsModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   GET  for retreiving info on the replication level of a family
     *   SET  for updating a value or the replication level of a family
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, HealthMonitorTask::Ptr const& healthMonitorTask,
                        std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpReplicationLevelsModule() = delete;
    HttpReplicationLevelsModule(HttpReplicationLevelsModule const&) = delete;
    HttpReplicationLevelsModule& operator=(HttpReplicationLevelsModule const&) = delete;

    ~HttpReplicationLevelsModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpReplicationLevelsModule(Controller::Ptr const& controller, std::string const& taskName,
                                HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                qhttp::Response::Ptr const& resp,
                                HealthMonitorTask::Ptr const& healthMonitorTask);

    /// @return nlohmann::json The replica status report for all known families.
    nlohmann::json _get();

    /// Set the replication level of a family.
    /// @return nlohmann::json The replica status report for all known families.
    nlohmann::json _set();

    /// Update the cached replication report (if any).
    /// @param force Ignore the cache and rebuild the report if 'true'.
    /// @return nlohmann::json The latest state of the report.
    nlohmann::json _makeReport(bool force = false);

    std::weak_ptr<HealthMonitorTask> const _healthMonitorTask;

    // The cached state is shared by all instances of the class

    /// The cached state of the last replication levels report
    static nlohmann::json _replicationLevelReport;

    /// The time of the last cached report
    static uint64_t _replicationLevelReportTimeMs;

    /// Protects the replication level cache
    static replica::Mutex _replicationLevelMtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPREPLICATIONLEVELSMODULE_H
