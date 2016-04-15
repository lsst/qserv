/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// Class header
#include "czar/Czar.h"

// System headers
#include <sys/time.h>
#include <thread>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "czar/MessageTable.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");

// parse KILL query, return thread ID or -1
int parseKillQuery(std::string const& query);

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace czar {

// Constructors
Czar::Czar(std::string const& configPath, std::string const& czarName)
    : _czarName(czarName), _czarConfig(configPath),
      _idCounter(), _uqFactory(), _clientToQuery(), _mutex() {

    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const int year = 60*60*24*365;
    _idCounter = uint64_t(tv.tv_sec % year)*1000 + tv.tv_usec/1000;

    std::string logConfig = _czarConfig.getLogConfig();
    if (not logConfig.empty()) {
        LOG_CONFIG(logConfig);
    }

    LOGS(_log, LOG_LVL_INFO, "Creating czar instance with name " << czarName);
    LOGS(_log, LOG_LVL_DEBUG, "Czar config: " << _czarConfig);

    _uqFactory.reset(new ccontrol::UserQueryFactory(_czarConfig, _czarName));
}

SubmitResult
Czar::submitQuery(std::string const& query,
                  std::map<std::string, std::string> const& hints) {

    LOGS(_log, LOG_LVL_INFO, "New query: " << query
         << ", hints: " << util::printable(hints));

    util::ConfigStore hintsConfigStore(hints);

    // Analyze query hints
    std::string clientId = hintsConfigStore.getStringOrDefault("client_dst_name");

    // Not being able to get thread id is not fatal,
    // it just means query cannot be associate with particular
    // client/thread and will not be able to be killed later
    int threadId = hintsConfigStore.getIntOrDefault("server_thread_id", -1);

    std::string defaultDb = hintsConfigStore.getStringOrDefault("db");
    LOGS(_log, LOG_LVL_DEBUG, "Default database is \"" << defaultDb <<"\"");



    // make message table name
    std::string userQueryId = std::to_string(_idCounter++);
    LOGS(_log, LOG_LVL_DEBUG, "userQueryId: " << userQueryId);
    std::string resultDb = _czarConfig.getMySqlResultConfig().dbName;
    std::string const lockName = resultDb + ".message_" + userQueryId;

    SubmitResult result;

    // instantiate message table manager
    MessageTable msgTable(lockName, _czarConfig.getMySqlResultConfig());
    try {
        msgTable.lock();
    } catch (std::exception const& exc) {
        result.errorMessage = exc.what();
        return result;
    }


    // make new UserQuery
    // this is atomic
    ccontrol::UserQuery::Ptr uq;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        uq = _uqFactory->newUserQuery(query, defaultDb);
    }

    // check for errors
    auto error = uq->getError();
    if (not error.empty()) {
        result.errorMessage = "Failed to instantiate query: " + error;
        return result;
    }

    // spawn background thread to wait until query finishes to unlock,
    // note that lambda stores copies of uq and msgTable.
    auto finalizer = [uq, msgTable]() mutable {
        LOGS(_log, LOG_LVL_DEBUG, "submitting new query");
        uq->submit();
        uq->join();
        try {
            msgTable.unlock(uq);
            if (uq) uq->discard();
        } catch (std::exception const& exc) {
            // TODO? if this fails there is no way to notify client, and client
            // will likely hang because table may still be locked.
            LOGS(_log, LOG_LVL_ERROR, "Query finalization failed (client likely hangs): " << exc.what());
        }
    };
    LOGS(_log, LOG_LVL_DEBUG, "starting finalizer thread for query");
    std::thread finalThread(finalizer);
    finalThread.detach();

    {
        std::lock_guard<std::mutex> lock(_mutex);

        // first cleanup client query map from completed queries
        for (auto iter = _clientToQuery.begin(); iter != _clientToQuery.end(); ) {
            if (iter->second.expired()) {
                iter = _clientToQuery.erase(iter);
            } else {
                ++ iter;
            }
        }

        // remember query (weak pointer) in case we want to kill query
        if (not clientId.empty() and threadId >= 0) {
            ClientThreadId ctId(clientId, threadId);
            _clientToQuery.insert(std::make_pair(ctId, uq));
            LOGS(_log, LOG_LVL_DEBUG, "Remembering query: (" << clientId << ", "
                 << threadId << ") (new map size: " << _clientToQuery.size() << ")");
        }
    }

    // return all info to caller
    if (not uq->getResultTableName().empty()) {
        result.resultTable = resultDb + "." + uq->getResultTableName();
    }
    result.messageTable = lockName;
    result.orderBy = uq->getProxyOrderBy();
    LOGS(_log, LOG_LVL_DEBUG, "returning result to proxy: resultTable="
         << result.resultTable << " messageTable=" << result.messageTable
         << " orderBy=" << result.orderBy);

    return result;
}

std::string
Czar::killQuery(std::string const& query, std::string const& clientId) {

    LOGS(_log, LOG_LVL_INFO, "KILL query: " << query << ", clientId: " << clientId);

    // the query can be one of:
    //   "KILL QUERY NNN" - kills currently running query in thread NNN
    //   "KILL CONNECTION NNN" - kills connection associated with thread NNN
    //                           and all queries in that connection
    //   "KILL NNN" - same as "KILL CONNECTION NNN"

    int threadId = ::parseKillQuery(query);
    LOGS(_log, LOG_LVL_DEBUG, "thread ID: " << threadId);
    if (threadId < 0) {
        return "Failed to parse query: " + query;
    }

    ClientThreadId ctId(clientId, threadId);
    ccontrol::UserQuery::Ptr uq;

    {
        std::lock_guard<std::mutex> lock(_mutex);

        // find it in the client map based in client/thread id
        auto iter = _clientToQuery.find(ctId);
        if (iter == _clientToQuery.end()) {
            return "Unknown thread ID: " + query;
        }
        uq = iter->second.lock();
    }

    // assume this cannot fail or throw
    LOGS(_log, LOG_LVL_DEBUG, "Killing query for thread: " << threadId);
    if (uq) {
        uq->kill();
    }

    return std::string();
}

}}} // namespace lsst::qserv::czar

namespace {

int
parseKillQuery(std::string const& aQuery) {
    // the query that proxy passes us is all uppercase and spaces compressed
    // but it may have trailing space which we strip first
    std::string query = aQuery;
    auto pos = query.find_last_not_of(' ');
    query.erase(pos+1);

    // try to match against one or another form of KILL
    static const std::string prefixes[] = {"KILL QUERY ", "KILL CONNECTION ", "KILL "};
    for (auto& prefix: prefixes) {
        LOGS(_log, LOG_LVL_DEBUG, "checking prefix: '" << prefix << "'");
        if (query.compare(0, prefix.size(), prefix) == 0) {
            LOGS(_log, LOG_LVL_DEBUG, "match found");
            try {
                LOGS(_log, LOG_LVL_DEBUG, "thread id: '" << query.substr(prefix.size()) << "'");
                return boost::lexical_cast<int>(query.substr(prefix.size()));
            } catch (boost::bad_lexical_cast const& exc) {
                // error in query syntax
                return -1;
            }
        }
    }

    return -1;
}

} // namespace
