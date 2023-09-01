/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CZAR_CZAR_H
#define LSST_QSERV_CZAR_CZAR_H

// System headers
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "ccontrol/UserQueryFactory.h"
#include "czar/SubmitResult.h"
#include "global/intTypes.h"
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "qdisp/SharedResources.h"
#include "util/ConfigStore.h"
#include "util/Timer.h"

namespace lsst::qserv {

namespace cconfig {
class CzarConfig;
}

namespace qdisp {
class PseudoFifo;
}

namespace util {
class FileMonitor;
}

namespace czar {

/// @addtogroup czar

/**
 * @ingroup czar
 * @brief Class representing czar "entry points".
 */

class Czar {
public:
    using Ptr = std::shared_ptr<Czar>;

    Czar(Czar const&) = delete;
    Czar& operator=(Czar const&) = delete;

    /**
     * Submit query for execution.
     *
     * @param query: Query text.
     * @param hints: Optional query hints, default database name should be
     *               provided as "db" key.
     * @return Structure with info about submitted query.
     */
    SubmitResult submitQuery(std::string const& query, std::map<std::string, std::string> const& hints);

    /**
     * Process a kill query command (experimental).
     *
     * @param query: (client)proxy-provided "KILL QUERY ..." string
     * @param clientId : client name from proxy
     * @throws Exception is thrown if query Id is not known
     */
    void killQuery(std::string const& query, std::string const& clientId);

    /**
     * Make new instance.
     *
     * @param configPath:   Path to the configuration file.
     * @param czarName:     Name if this instance, must be unique. If empty name
     *                      is given then random name will be constructed.
     */
    static Ptr createCzar(std::string const& configPath, std::string const& czarName);

    /**
     * During startup, this may return nullptr.
     *
     * @return a pointer to the czar.
     *
     */
    static Ptr getCzar() { return _czar; }

    /// Return a pointer to QdispSharedResources
    qdisp::SharedResources::Ptr getQdispSharedResources() { return _qdispSharedResources; }

    /// Remove all old tables in the qservResult database.
    void removeOldResultTables();

    /// @return true if trivial queries should be treated as
    ///         interactive queries to stress test the czar.
    bool getQueryDistributionTestVer() { return _queryDistributionTestVer; }

protected:
private:
    /// Private constructor for singleton.
    Czar(std::string const& configPath, std::string const& czarName);

    /// Clean query maps from expired entries, _mutex must be locked
    void _cleanupQueryHistoryLocked();

    /// Clean query maps from expired entries
    void _cleanupQueryHistory();

    /// Clean query maps from expired entries, add new query
    void _updateQueryHistory(std::string const& clientId, int threadId, ccontrol::UserQuery::Ptr const& uq);

    /// Create and fill async result table
    void _makeAsyncResult(std::string const& asyncResultTable, QueryId queryId, std::string const& resultLoc);

    /// @return An identifier of the last query that was recorded in the query metadata table
    QueryId _lastQueryIdBeforeRestart() const;

    static Ptr _czar;  ///< Pointer to single instance of the Czar.

    // combines client name (ID) and its thread ID into one unique ID
    typedef std::pair<std::string, int> ClientThreadId;
    typedef std::map<ClientThreadId, std::weak_ptr<ccontrol::UserQuery>> ClientToQuery;
    typedef std::map<QueryId, std::weak_ptr<ccontrol::UserQuery>> IdToQuery;

    std::string const _czarName;  ///< Unique czar name
    std::shared_ptr<cconfig::CzarConfig> const _czarConfig;

    std::atomic<uint64_t> _idCounter;  ///< Query/task identifier for next query
    std::unique_ptr<ccontrol::UserQueryFactory> _uqFactory;
    ClientToQuery _clientToQuery;  ///< maps client ID to query
    IdToQuery _idToQuery;          ///< maps query ID to query (for currently running queries)
    std::mutex _mutex;             ///< protects _uqFactory, _clientToQuery, and _idToQuery

    /// Thread pool for handling Responses from XrdSsi,
    /// the PsuedoFifo to prevent czar from calling most recent requests,
    /// and any other resources for use by query executives.
    qdisp::SharedResources::Ptr _qdispSharedResources;

    util::Timer _lastRemovedTimer;  ///< Timer to limit table deletions.
    std::mutex _lastRemovedMtx;     ///< protects _lastRemovedTimer

    /// Prevents multiple concurrent calls to _removeOldTables().
    std::atomic<bool> _removingOldTables{false};
    std::thread _oldTableRemovalThread;  ///< thread needs to remain valid while running.

    bool _queryDistributionTestVer;  ///< True if config says this is distribution test version.

    /// Reloads the log configuration file on log config file change.
    std::shared_ptr<util::FileMonitor> _logFileMonitor;
};

}  // namespace czar
}  // namespace lsst::qserv

#endif  // LSST_QSERV_CZAR_CZAR_H
