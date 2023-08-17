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

// Forward declarations
namespace lsst::qserv::wbase {
struct TaskSelector;
}  // namespace lsst::qserv::wbase

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
     *   WORKERS    - get the status info of many workers
     *   WORKER     - get the status info of a specific worker
     *   WORKER-DB  - get the database status of a specific worker
     *   CZAR       - get the status info of Czar
     *   CZAR-DB    - get the database status of Czar
     *   QUERIES-ACTIVE           - get user query info on the on-going queries
     *   QUERIES-ACTIVE-PROGRESS  - get the progression history (of the active queries)
     *   QUERIES-PAST             - search and display info on the past queries
     *   QUERY                    - get user query info for a specific query
     *   CSS        - get CSS configurations (the shared scan settings, etc.)
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
     * Process a request for extracting various status info on the database
     * service for select Qserv worker.
     */
    nlohmann::json _workerDb();

    /**
     * Process a request for extracting various status info of Czar.
     */
    nlohmann::json _czar();

    /**
     * Process a request for extracting various status info on the database
     * service of Czar.
     */
    nlohmann::json _czarDb();

    /**
     * Process a request for extracting a status on select user queries
     * launched at Qserv.
     */
    nlohmann::json _activeQueries();

    /**
     * Process a request for extracting the progression history on the active
     * user queries that are being executed by Qserv.
     */
    nlohmann::json _activeQueriesProgress();

    /**
     * Process a request for extracting a status on the past (finished/failed) user
     * queries submitted to Qserv.
     */
    nlohmann::json _pastQueries();

    /**
     * Process a request for extracting a status on a specific user query
     * launched at Qserv.
     */
    nlohmann::json _userQuery();

    /**
     * Extract and parse values of the worker task selector.
     */
    wbase::TaskSelector _translateTaskSelector(std::string const& func) const;

    /**
     * The helper method for processing the input JSON object and populating
     * the output collections.
     * @note The method is shared by implementations of _worker() and _workers(), and
     * it's needed to avoid code duplication.
     */
    void _processWorkerInfo(std::string const& worker, bool keepResources, nlohmann::json const& inWorkerInfo,
                            nlohmann::json& statusRef,
                            std::map<std::string, std::set<int>>& schedulers2chunks,
                            std::set<int>& chunks) const;

    /**
     * The helper method translates the input collection into the JSON reptresentation.
     * @note The method is shared by implementations of _worker() and _workers(), and
     * it's needed to avoid code duplication.
     */
    nlohmann::json _schedulers2chunks2json(
            std::map<std::string, std::set<int>> const& schedulers2chunks) const;

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
    nlohmann::json _getQueries(nlohmann::json const& workerInfo) const;

    /// @return The CSS info (shared scan parameters of all partitioned tables, etc.)
    nlohmann::json _css();

    /**
     * @param chunks  collection of chunks numbers to be expanded
     * @return descriptors of chunks (including their spatial geometry)
     */
    nlohmann::json _chunkInfo(std::set<int> const& chunks) const;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPQSERVMONITORMODULE_H
