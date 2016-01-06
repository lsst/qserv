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
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "czar/MessageTable.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");

// read configuration file
lsst::qserv::StringMap readConfig(std::string const& configPath);

// parse KILL query, return thread ID or -1
int parseKillQuery(std::string const& query);

// make mysql config object from config map
lsst::qserv::mysql::MySqlConfig mysqlConfig(lsst::qserv::StringMap const& config);

} // namespace


namespace lsst {
namespace qserv {
namespace czar {

// Constructors
Czar::Czar(std::string const& configPath, std::string const& czarName)
    : _czarName(czarName), _config(::readConfig(configPath)),
      _resultConfig(::mysqlConfig(_config)), _idCounter(),
      _uqFactory(), _clientToQuery(), _mutex() {

    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const int year = 60*60*24*365;
    _idCounter = uint64_t(tv.tv_sec % year)*1000 + tv.tv_usec/1000;

    ccontrol::ConfigMap cm(_config);
    std::string logConfig = cm.get("log.logConfig", "", "");
    if (not logConfig.empty()) {
        LOG_CONFIG(logConfig);
    }

    LOGS(_log, LOG_LVL_INFO, "creating czar instance with name " << czarName);
    LOGS(_log, LOG_LVL_INFO, "czar config: " << util::printable(_config));

    _uqFactory.reset(new ccontrol::UserQueryFactory(_config, _czarName));
}

SubmitResult
Czar::submitQuery(std::string const& query,
                  std::map<std::string, std::string> const& hints) {

    LOGS(_log, LOG_LVL_INFO, "New query: " << query
         << ", hints: " << util::printable(hints));

    // get some info from hints
    std::string clientId;
    int threadId = -1;

    auto hintIter = hints.find("client_dst_name");
    if (hintIter != hints.end()) clientId = hintIter->second;

    hintIter = hints.find("server_thread_id");
    if (hintIter != hints.end()) {
        try {
            threadId = boost::lexical_cast<int>(hintIter->second);
        } catch (boost::bad_lexical_cast const& exc) {
            // Not fatal, just means we cannot associate query with particular
            // client/thread and will not be able to kill it later
        }
    }

    // this is atomic
    uint64_t userQueryId = _idCounter++;
    LOGS(_log, LOG_LVL_DEBUG, "userQueryId: " << userQueryId);

    // make table names
    auto userQueryIdStr = std::to_string(userQueryId);
    std::string const resultName = _resultConfig.dbName + ".result_" + userQueryIdStr;
    std::string const lockName = _resultConfig.dbName + ".message_" + userQueryIdStr;

    SubmitResult result;

    // instantiate message table manager
    MessageTable msgTable(lockName, _resultConfig);
    try {
        msgTable.lock();
    } catch (std::exception const& exc) {
        result.errorMessage = exc.what();
        return result;
    }

    // make new UserQuery
    ccontrol::ConfigMap cm(hints);
    std::string defDb = cm.get("db", "Failed to find default database, using empty string", "");
    ccontrol::UserQuery::Ptr uq;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        uq = _uqFactory->newUserQuery(query, defDb, resultName, userQueryId);
    }

    // check for errors
    auto error = uq->getError();
    if (not error.empty()) {
        result.errorMessage = "Failed to instantiate query: " + error;
        return result;
    }

    // start execution
    LOGS(_log, LOG_LVL_DEBUG, "submitting new query");
    uq->submit();

    // spawn background thread to wait until query finishes to unlock,
    // note that lambda stores copies of uq and msgTable.
    auto finalizer = [uq, msgTable]() mutable {
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
    result.resultTable = resultName;
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

// read configuration file
lsst::qserv::StringMap
readConfig(std::string const& configPath) {
    using boost::property_tree::ini_parser::read_ini;
    using boost::property_tree::ptree;

    // read it into a ptree
    ptree pt;
    read_ini(configPath, pt);

    // flatten
    lsst::qserv::StringMap config;
    for (auto& sectionPair: pt) {
        auto& section = sectionPair.first;
        for (auto& itemPair: sectionPair.second) {
            auto& item = itemPair.first;
            auto& value = itemPair.second.data();
            config.insert(std::make_pair(section + "." + item, value));
        }
    }

    return config;
}

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

// make mysql config object from config map
lsst::qserv::mysql::MySqlConfig
mysqlConfig(lsst::qserv::StringMap const& config) {

    lsst::qserv::ccontrol::ConfigMap cm(config);

    lsst::qserv::mysql::MySqlConfig mysqlConfig;

    mysqlConfig.hostname = cm.get(
        "resultdb.host",
        "Error, resultdb.host not found. Using empty host name.",
        "");
    mysqlConfig.port = cm.getTyped<unsigned>(
        "resultdb.port",
        "Error, resultdb.port not found. Using 0 for port.",
        0U);
    mysqlConfig.username = cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    mysqlConfig.password = cm.get(
        "resultdb.passwd",
        "Error, resultdb.passwd not found. Using empty string.",
        "");
    mysqlConfig.socket = cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using empty string.",
        "");
    mysqlConfig.dbName = cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservMeta.",
        "qservResult");

    return mysqlConfig;
}

} // namespace
