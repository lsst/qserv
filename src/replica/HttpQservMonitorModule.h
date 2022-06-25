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
#ifndef LSST_QSERV_HTTPQSERVMONITORMODULE_H
#define LSST_QSERV_HTTPQSERVMONITORMODULE_H

// System headers
#include <map>
#include <memory>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "replica/DatabaseMySQL.h"
#include "replica/HttpModule.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpQservMonitorModule implements a handler for reporting
 * various monitoring stats on info on a managed instance of Qserv.
 */
class HttpQservMonitorModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpQservMonitorModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   WORKERS                for many workers (possible selected by various criteria)
     *   SELECT-WORKER-BY-NAME  for a specific worker
     *   QUERIES                for many queries (selected by various criteria)
     *   SELECT-QUERY-BY-ID     for a specific query
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        HttpAuthType const authType = HttpAuthType::NONE);

    HttpQservMonitorModule() = delete;
    HttpQservMonitorModule(HttpQservMonitorModule const&) = delete;
    HttpQservMonitorModule& operator=(HttpQservMonitorModule const&) = delete;

    ~HttpQservMonitorModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpQservMonitorModule(Controller::Ptr const& controller, std::string const& taskName,
                           HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                           qhttp::Response::Ptr const& resp);

    /**
     * Process a request for extracting various status info for select
     * Qserv workers (all of them or a subset of those as per parameters
     * of a request).
     */
    nlohmann::json _workers();

    /**
     * Process a request for extracting various status info for one
     * Qserv worker.
     */
    nlohmann::json _worker();

    /**
     * Process a request for extracting a status on select user queries
     * launched at Qserv.
     */
    nlohmann::json _userQueries();

    /**
     * Process a request for extracting a status on a specific user query
     * launched at Qserv.
     */
    nlohmann::json _userQuery();

    /**
     * @brief Extract info on the ongoing queries.
     * @param conn Database connection to the Czar database.
     * @param queryId2scheduler The map with optional entries indicating which schedulers
     *   are used by Qserv workers for processing the corresponding queries.
     * @return nlohmann::json A collection queries found in the database.
     */
    nlohmann::json _currentUserQueries(database::mysql::Connection::Ptr& conn,
                                       std::map<QueryId, std::string> const& queryId2scheduler);

    /**
     * @brief Extract info on the user queries.
     * @param conn Database connection to the Czar database.
     * @param constraint The constraint for the connections to look for.
     * @param limit4past The maximum number of queries to be reported.
     * @param includeMessages If the flag is set to 'true' then persistent
     *   messages reported by Czar for each query will be included into the result.
     * @return nlohmann::json A collection queries found in the database for
     *   the specified constraint.
     */
    nlohmann::json _pastUserQueries(database::mysql::Connection::Ptr& conn, std::string const& constraint,
                                    unsigned int limit4past, bool includeMessages);

    /**
     * Find descriptions of queries
     *
     * @param workerInfo  worker info object to be inspected to extract identifier)s of queries
     * @return descriptions of the queries
     */
    nlohmann::json _getQueries(nlohmann::json& workerInfo) const;

    /**
     * @param chunks  collection of chunks numbers to be expanded
     * @return descriptors of chunks (including their spatial geometry)
     */
    nlohmann::json _chunkInfo(std::set<int> const& chunks) const;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPQSERVMONITORMODULE_H
