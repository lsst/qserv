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
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/userQueryProxy.h"
#include "czar/MessageTable.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.Czar");

// read configuration file
lsst::qserv::StringMap readConfig(std::string const& configPath);

}


namespace lsst {
namespace qserv {
namespace czar {

// Constructors
Czar::Czar(std::string const& configPath, std::string const& czarName)
    : _czarName(czarName), _config(::readConfig(configPath)), _idCounter(),
      _resultConfig(), _uqFactory() {

    // set id counter to milliseconds since the epoch, mod 1 year.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    _idCounter = unsigned((tv.tv_sec % (60*60*24*365)) + tv.tv_usec/1000);

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
        result[0] = "Failed to lock message table: " + std::string(exc.what());
        return result;
    }

    // make new UserQuery
    ccontrol::ConfigMap cm(hints);
    std::string defDb = cm.get("db", "Failed to find default database, using empty string", "");
    auto qPair = _uqFactory->newUserQuery(query, defDb, resultName);
    auto session = qPair.first;
    auto& orderBy = qPair.second;

    // check for errors
    auto error = ccontrol::UserQuery_getQueryProcessingError(session);
    if (not error.empty()) {
        result[0] = "Failed to instantiate query: " + error;
        return result;
    }

    // start execution
    ccontrol::UserQuery_submit(session);

    // spawn background thread to wait until query finishes to unlock
    auto finalizer = [](int session, MessageTable msgTable) {
        ccontrol::UserQuery_join(session);
        msgTable.unlock();
    };
    std::thread finalThread(finalizer, session, msgTable);
    finalThread.detach();

    // return all info to caller
    result[1] = resultName;
    result[2] = lockName;
    result[3] = orderBy;
    return result;
}

void
Czar::killQueryUgly(std::string const& query, std::string const& clientId) {

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

}
