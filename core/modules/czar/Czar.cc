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

}


namespace lsst {
namespace qserv {
namespace czar {

// Constructors
Czar::Czar(std::string const& configPath, std::string const& czarName)
    : _czarName(czarName), _config(::readConfig(configPath)), _idCounter(),
      _resultConfig(), _uqFactory(), _clientToQuery() {

    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const int year = 60*60*24*365;
    _idCounter = unsigned((tv.tv_sec % year)*1000 + tv.tv_usec/1000);

    ccontrol::ConfigMap cm(_config);
    std::string logConfig = cm.get("log.logConfig", "", "");
    if (not logConfig.empty()) {
        LOG_CONFIG(logConfig);
    }

    LOGF(_log, LOG_LVL_INFO, "creating czar instance with name %s" % czarName);
    LOGF(_log, LOG_LVL_DEBUG, "czar config: %s" % util::printable(_config));

    _uqFactory.reset(new ccontrol::UserQueryFactory(_config, _czarName));

    _resultConfig.hostname = cm.get(
        "resultdb.host",
        "Error, resultdb.host not found. Using empty host name.",
        "");
    _resultConfig.port = cm.getTyped<unsigned>(
        "resultdb.port",
        "Error, resultdb.port not found. Using 0 for port.",
        0U);
    _resultConfig.username = cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    _resultConfig.password = cm.get(
        "resultdb.passwd",
        "Error, resultdb.passwd not found. Using empty string.",
        "");
    _resultConfig.socket = cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using empty string.",
        "");
    _resultConfig.dbName = cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservMeta.",
        "qservResult");
}

std::vector<std::string>
Czar::submitQuery(std::string const& query,
                  std::map<std::string, std::string> const& hints) {

    LOGF(_log, LOG_LVL_INFO, "new query: %s" % query);
    LOGF(_log, LOG_LVL_INFO, "hints: %s" % util::printable(hints));

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

    unsigned taskId = _idCounter++;
    LOGF(_log, LOG_LVL_DEBUG, "taskId: %s" % taskId);

    // make table names
    auto taskIdStr = std::to_string(taskId);
    std::string const resultName = _resultConfig.dbName + ".result_" + taskIdStr;
    std::string const lockName = _resultConfig.dbName + ".message_" + taskIdStr;

    std::vector<std::string> result(4);

    // instantiate message table manager
    MessageTable msgTable(lockName, _resultConfig);
    try {
        msgTable.lock();
    } catch (std::exception const& exc) {
        result[0] = exc.what();
        return result;
    }

    // make new UserQuery
    ccontrol::ConfigMap cm(hints);
    std::string defDb = cm.get("db", "Failed to find default database, using empty string", "");
    auto uq = _uqFactory->newUserQuery(query, defDb, resultName);

    // check for errors
    auto error = uq->getError();
    if (not error.empty()) {
        result[0] = "Failed to instantiate query: " + error;
        return result;
    }

    // start execution
    LOGF(_log, LOG_LVL_DEBUG, "submitting new query");
    uq->submit();

    // spawn background thread to wait until query finishes to unlock
    auto finalizer = [](ccontrol::UserQuery::Ptr uq, MessageTable msgTable) {
        uq->join();
        try {
            msgTable.unlock(uq);
        } catch (std::exception const& exc) {
            // TODO: if this fails there is no way to notify client, and client
            // will likely hang because table will still be locked.
        }
    };
    LOGF(_log, LOG_LVL_DEBUG, "starting finalizer thread for query");
    std::thread finalThread(finalizer, uq, msgTable);
    finalThread.detach();

    // remember query (weak pointer) in case we want to kill query
    // TODO: the map grows indefinitely now, will have to do some cleanup
    if (not clientId.empty() and threadId >= 0) {
        ClientThreadId ctId(clientId, threadId);
        _clientToQuery.insert(std::make_pair(ctId, uq));
        LOGF(_log, LOG_LVL_DEBUG, "Remembering query: (%s, %s) (new map size: %s)" %
             clientId % threadId % _clientToQuery.size());
    }

    // return all info to caller
    result[1] = resultName;
    result[2] = lockName;
    result[3] = uq->getProxyOrderBy();
    LOGF(_log, LOG_LVL_DEBUG, "returning result to proxy: %s" % util::printable(result));
    return result;
}

std::string
Czar::killQuery(std::string const& query, std::string const& clientId) {

    LOGF(_log, LOG_LVL_INFO, "KILL query: '%s'" % query);
    LOGF(_log, LOG_LVL_INFO, "client ID: '%s'" % clientId);

    // the query can be one of:
    //   "KILL QUERY NNN" - kills currently running query in thread NNN
    //   "KILL CONNECTION NNN" - kills connection associated with thread NNN
    //                           and all queries in that connection
    //   "KILL NNN" - same as "KILL CONNECTION NNN"

    int threadId = ::parseKillQuery(query);
    LOGF(_log, LOG_LVL_INFO, "thread ID: %s" % threadId);
    if (threadId < 0) {
        return "Failed to parse query: " + query;
    }

    ClientThreadId ctId(clientId, threadId);

    auto iter = _clientToQuery.find(ctId);
    if (iter == _clientToQuery.end()) {
        return "Unknown thread ID: " + query;
    }

    // assume this cannot fail or throw
    auto uq = iter->second.lock();
    LOGF(_log, LOG_LVL_INFO, "Killing query for thread: %s" % threadId);
    uq->kill();

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
        LOGF(_log, LOG_LVL_INFO, "checking prefix: '%s'" % prefix);
        if (query.compare(0, prefix.size(), prefix) == 0) {
            LOGF(_log, LOG_LVL_INFO, "match found");
            try {
                LOGF(_log, LOG_LVL_INFO, "thread id: '%s'" % query.substr(prefix.size()));
                return boost::lexical_cast<int>(query.substr(prefix.size()));
            } catch (boost::bad_lexical_cast const& exc) {
                // error in query syntax
                return -1;
            }
        }
    }

    return -1;
}

}
