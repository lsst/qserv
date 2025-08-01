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
#ifndef LSST_QSERV_QMETA_QPROGRESSPLOT_H
#define LSST_QSERV_QMETA_QPROGRESSPLOT_H

// System headers
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

// Qserv headers
#include "global/intTypes.h"
#include "mysql/MySqlConfig.h"

// Third party headers
#include <nlohmann/json.hpp>

// Forward declarations

namespace lsst::qserv::sql {
class SqlConnection;
}  // namespace lsst::qserv::sql

// This header declarations
namespace lsst::qserv::qmeta {

/**
 * Class QProgressPlot manages the query progress plots in memory and in
 * the metadata database.
 *
 * The plots of the ongoing queries are stored in memory and updated
 * periodically. The plots of the completed queries are archived in
 * the table QProgressPlot.
 *
 * @note The class has a singleton-like behavior. The instance of the class
 * is created on the first call to the static method create() and then
 * the same instance is returned on subsequent calls to the same method or to
 * the static method get().
 * @note The class has the thread-safe implementation.
 */
class QProgressPlot {
public:
    /**
     * Factory method to create a QProgressPlot instance.
     * @param mysqlConf MySQL configuration.
     * @return The shared pointer to the QProgressPlot instance. The method will return
     *  a shared pointer to the instance regardless if it was created during the call or
     *  it was done before.
     */
    static std::shared_ptr<QProgressPlot> create(mysql::MySqlConfig const& mysqlConf);

    /**
     * @return The shared pointer to the QProgressPlot instance or nullptr if not created.
     */
    static std::shared_ptr<QProgressPlot> get();

    QProgressPlot() = delete;
    QProgressPlot(QProgressPlot const&) = delete;
    QProgressPlot& operator=(QProgressPlot const&) = delete;

    ~QProgressPlot() = default;

    /**
     * Begin tracking the specified query.
     * @param qid The unique identifier of a query affected by the operation.
     */
    void track(QueryId qid);

    /**
     * Update the query counter(s).
     * @note The method will only record changes in the counter of jobs if
     *  the provided number differs from the previously recorded value.
     * @param qid The unique identifier of a query affected by the operation.
     * @param numUnfinishedJobs The number of unfinished jobs.
     * @throw qmeta::ConsistencyError if the query is not being tracked.
     */
    void update(QueryId qid, int numUnfinishedJobs);

    /**
     * Finish tracking the specified query and archive the history in the database.
     * @param qid The unique identifier of a query affected by the operation.
     * @throw qmeta::ConsistencyError if the query is not being tracked.
     */
    void untrack(QueryId qid);

    /**
     * Get info on a progress of a query.
     * @param qid The unique identifier of a query.
     * @return The JSON object encapsulating the query progress and the status of the query.
     * The schema of the result object is as follows:
     * @code
     *   { "queryId": <number>,
     *     "status":  <string>,
     *     "history": [
     *       [<time_ms>,<num_jobs>],
     *       ...
     *     ]
     *   }
     * @endcode
     * @throw qmeta::ConsistencyError if the query is unknown.
     */
    nlohmann::json get(QueryId qid) const;

    /**
     * Get info on a progress of the select queries.
     * @param lastSeconds The cut-off time for the age of entries to be reported.
     *  A value of the parameter is used to determined the time interval [ now - lastSeconds, now ],
     *  where "now" is the current time.
     * @param includeFinished If true, include finished queries in the result.
     *  If false, only ongoing queries will be included.
     * @return The JSON object encapsulating the query progress and the status of the query.
     * The schema of the result object is as follows:
     * @code
     *   [ { "queryId": <number>,
     *       "status":  <string>,
     *       "history": [
     *         [<time_ms>,<num_jobs>],
     *         ...
     *       ]
     *     },
     *     ...
     *   ]
     * @endcode
     * @throw util::Issue if the cut-off time is 0 and includeFinished is true.
     */
    nlohmann::json get(unsigned int lastSeconds, bool includeFinished = false) const;

private:
    class HistoryPoint {
    public:
        HistoryPoint(std::uint64_t timeMs_ = 0, int numJobs_ = 0) : timeMs(timeMs_), numJobs(numJobs_) {}
        std::uint64_t timeMs = 0;
        int numJobs = 0;
    };

    QProgressPlot(mysql::MySqlConfig const& mysqlConf);

    /// Archive the history of a query in the database
    /// @param qid The unique identifier of a query.
    /// @param history The history of the query to be archived.
    void _archive(QueryId qid, std::list<HistoryPoint> const& history);

    static std::shared_ptr<QProgressPlot> _instance;  ///< The singleton instance of the class.
    static std::mutex _instanceMutex;                 ///< Mutex for the singleton instance.

    std::uint64_t const _createdTimeMs;               ///< The time when the instance was created.
    std::shared_ptr<sql::SqlConnection> const _conn;  ///< Database connection.

    mutable std::mutex _mtx;      ///< For synchronized access to operations with ongoing queries.
    mutable std::mutex _connMtx;  ///< For synchronized access to the database connection.

    /// Ongoing query progress plots.
    std::unordered_map<QueryId, std::list<HistoryPoint>> _executing;
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QPROGRESSPLOT_H
