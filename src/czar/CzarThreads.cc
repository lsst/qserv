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

// Class header
#include "czar/CzarThreads.h"

// System headers
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// Third-party headers
#include <boost/algorithm/string/replace.hpp>
#include <nlohmann/json.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "qmeta/QMeta.h"
#include "sql/SqlErrorObject.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "util/common.h"
#include "util/TimeUtils.h"

using namespace std;
using namespace std::chrono_literals;
using namespace nlohmann;

#define CONTEXT_ ("Czar::" + string(__func__) + " ")
#define DEBUG_(arg) LOGS(_log, LOG_LVL_DEBUG, CONTEXT_ << arg)
#define ERROR_(arg) LOGS(_log, LOG_LVL_ERROR, CONTEXT_ << arg)
#define WARN_(arg) LOGS(_log, LOG_LVL_WARN, CONTEXT_ << arg)

namespace {
// Messages are logged in the same way as in the Czar class (see Czar.cc).
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");
}  // anonymous namespace

namespace lsst::qserv::czar {

inline string searchForOldTablesQuery(string const& resultDbName, int const resultAgeDay) {
    return "SELECT table_name,create_time FROM information_schema.tables WHERE table_schema='" +
           resultDbName +
           "' AND engine IS NOT NULL AND ((update_time IS NOT NULL AND update_time < NOW() - INTERVAL " +
           to_string(resultAgeDay) + " DAY) OR (update_time IS NULL AND create_time < NOW() - INTERVAL " +
           to_string(resultAgeDay) + " DAY))";
}

void garbageCollection(shared_ptr<cconfig::CzarConfig> czarConfig) {
    while (true) {
        // Sanitize values of the thresholds to tolerate a misconfiguration of Czar.
        // Sample values of the table ages at each iteration of the loop to allow the dynamic
        // reconfiguration of Czar w/o restart.
        int const resultAgeDay = max(1, czarConfig->getOldestResultKeptDays());
        int const asyncResultAgeSec = max(3600, czarConfig->getOldestAsyncResultKeptSeconds());
        string const query = searchForOldTablesQuery(czarConfig->getMySqlResultConfig().dbName, resultAgeDay);
        DEBUG_("Searching for tables older than " << resultAgeDay << " days, query: " << query);
        try {
            sql::SqlResults results;
            sql::SqlErrorObject err;
            auto sqlConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlResultConfig());
            if (!sqlConn->runQuery(query, results, err)) {
                ERROR_("Failed to locate old result tables, err: " << err.printErrMsg()
                                                                   << ", query: " << query);
            }
            vector<string> tables;
            vector<string> createTimes;
            results.extractFirst2Columns(tables, createTimes, err);
            for (size_t i = 0; i < tables.size(); ++i) {
                string const& table = tables[i];
                string const& createTime = createTimes[i];
                DEBUG_("Deleting old result table: " << table << ", created on: " << createTime);
                if (!sqlConn->runQuery("DROP TABLE IF EXISTS `" + table + "`", err)) {
                    ERROR_("Failed to delete old result table: " << table << ", err: " << err.printErrMsg());
                }
            }
        } catch (std::exception const& ex) {
            ERROR_("ex: " << ex.what());
        }
        // Make the next check after waiting for the full expiration age of the ASYNC tables.
        // The current implementaton of the garbage collection for the ASYNC queries makes it
        // a responsibility of the client. This opens a possibility that the client may
        // misbehave and not delete the ASYNC tables, which in turn may result in the substantial
        // growth in the number of unclaimed tables in the result database.
        this_thread::sleep_for(chrono::seconds(asyncResultAgeSec));
    }
}

void startGarbageCollect(shared_ptr<cconfig::CzarConfig> czarConfig) {
    thread t(garbageCollection, czarConfig);
    t.detach();
}

string searchForOldAsyncQuery(uint64_t const beginAgeSec, int const asyncResultAgeSec) {
    return "SELECT queryId,submitted,messageTable,resultLocation FROM QInfo"
           " WHERE qType = 'ASYNC' AND status != 'EXECUTING' AND"
           " ((completed IS NOT NULL AND completed > NOW() - INTERVAL " +
           to_string(beginAgeSec) + " SECOND AND completed < NOW() - INTERVAL " +
           to_string(asyncResultAgeSec) + " SECOND) OR (completed IS NULL AND submitted > NOW() - INTERVAL " +
           to_string(beginAgeSec) + " SECOND AND submitted < NOW() - INTERVAL " +
           to_string(asyncResultAgeSec) + " SECOND))";
}

void garbageCollectionAsync(shared_ptr<cconfig::CzarConfig> czarConfig) {
    // The algorithm implements a sliding window approach, where the width of the window
    // is dynamically adjusted based on the total duration of each iteration. This is done
    // to ensure that the algorithm does not consume too much CPU time and to avoid
    // executing redundant SQL queries for deleting tables that might have already been
    // deleted in the previous iteration of the loop.
    //
    // The algorithm relies on the time of the last check, recorded in prevTimeEpochSec.
    // This variable is updated upon completion of each iteration of the loop.
    // An initial value of 0 for the variable triggers a "deep" search in the query history,
    // covering a time range up to the point where the main garbage collection loop
    // is responsible for deleting outdated tables. The first interval to be evaluated
    // on the first pass (relative to the current time SQL NOW()) will be:
    //
    //   [ -(24 * 3600 * resultAgeDay) : -asyncResultAgeSec ]
    //
    // Each subsequent iteration will cover a shorter interval, based on the time of
    // the last check (relative to the current time SQL NOW()):
    //
    //   [ -(currTimeEpochSec - prevTimeEpochSec + asyncResultAgeSec + 1) : -asyncResultAgeSec ]

    uint64_t prevTimeEpochSec = 0;

    while (true) {
        // Sanitize values of the thresholds to tolerate a misconfiguration of Czar.
        // Sample values of the table ages at each iteration of the loop to allow the dynamic
        // reconfiguration of Czar w/o restart.
        int const resultAgeDay = max(1, czarConfig->getOldestResultKeptDays());
        int const asyncResultAgeSec = max(60, czarConfig->getOldestAsyncResultKeptSeconds());

        uint64_t const currTimeEpochSec = util::TimeUtils::nowSec();
        uint64_t const beginAgeSec = prevTimeEpochSec == 0
                                             ? 24 * 3600 * resultAgeDay
                                             : (currTimeEpochSec - prevTimeEpochSec) + asyncResultAgeSec + 1;
        string const query = searchForOldAsyncQuery(beginAgeSec, asyncResultAgeSec);
        DEBUG_("Searching for async queries newer than " << beginAgeSec << " seconds and older than "
                                                         << asyncResultAgeSec
                                                         << " seconds, query: " << query);
        try {
            sql::SqlResults results;
            sql::SqlErrorObject err;
            auto sqlQMetaConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlQmetaConfig());
            if (!sqlQMetaConn->runQuery(query, results, err)) {
                ERROR_("Failed to locate old result tables, err: " << err.printErrMsg()
                                                                   << ", query: " << query);
            }
            vector<string> queryIds;
            vector<string> submittedTimes;
            vector<string> messageTables;
            vector<string> resultLocations;
            results.extractFirst4Columns(queryIds, submittedTimes, messageTables, resultLocations, err);
            if (!queryIds.empty()) {
                auto sqlResultDbConn = sql::SqlConnectionFactory::make(czarConfig->getMySqlResultConfig());
                for (size_t i = 0; i < queryIds.size(); ++i) {
                    string const& queryId = queryIds[i];
                    string const& submittedTime = submittedTimes[i];
                    string const& messageTable = messageTables[i];
                    string resultLocation = resultLocations[i];
                    string resultTable;
                    if (resultLocation.compare(0, 6, "table:") == 0) {
                        boost::replace_all(resultLocation, "#QID#", queryId);
                        resultTable = resultLocation.substr(6);
                    } else {
                        throw runtime_error("Query queryId: " + queryId +
                                            " has unexpected result location: '" + resultLocation + "'");
                    }
                    DEBUG_("Deleting tables of old async query: "
                           << queryId << ", submitted on: " << submittedTime
                           << ", message table: " << messageTable << ", result table: " << resultTable);
                    for (auto const& table : {resultTable, messageTable}) {
                        if (!sqlResultDbConn->runQuery("DROP TABLE IF EXISTS `" + table + "`", err)) {
                            ERROR_("Failed to delete old result table: " << table
                                                                         << ", err: " << err.printErrMsg());
                        }
                    }
                }
            }
        } catch (std::exception const& ex) {
            ERROR_("ex: " << ex.what());
        }
        prevTimeEpochSec = util::TimeUtils::nowSec();

        // Waiting for the half period of the table expiration age before the next iteration.
        this_thread::sleep_for(chrono::seconds(asyncResultAgeSec / 2 + 1));
    }
}

void startGarbageCollectAsync(shared_ptr<cconfig::CzarConfig> czarConfig) {
    thread t(garbageCollectionAsync, czarConfig);
    t.detach();
}

void startGarbageCollectInProgress(shared_ptr<cconfig::CzarConfig> czarConfig, CzarId czarId,
                                   shared_ptr<qmeta::QMeta> queryMetadata) {
    // Sanitize a value of the configuration parameters to tolerate a misconfiguration of Czar.
    chrono::seconds const cleanupInterval = max(czarConfig->getInProgressCleanupIvalSec(), 1U) * 1s;
    thread t([czarConfig, czarId, queryMetadata, cleanupInterval]() {
        while (true) {
            queryMetadata->cleanupInProgressQueries(czarId);
            this_thread::sleep_for(cleanupInterval);
        }
    });
    t.detach();
}
}  // namespace lsst::qserv::czar
