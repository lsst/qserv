// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 AURA/LSST.
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
#include "wconfig/WorkerConfig.h"

// System headers
#include <sstream>
#include <stdexcept>

// Third party headers
#include <boost/algorithm/string/predicate.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStoreError.h"
#include "wsched/BlendScheduler.h"

using namespace std;
using namespace lsst::qserv::wconfig;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wconfig.WorkerConfig");
}  // namespace

namespace lsst::qserv::wconfig {

mutex WorkerConfig::_mtxOnInstance;

shared_ptr<WorkerConfig> WorkerConfig::_instance;

void ConfigVal::logValSet(std::string const& msg) {
    LOGS(_log, LOG_LVL_INFO, "ConfigVal " << getSectionName() << " set to " << getValStr() + " " + msg);
}

ConfigValResultDeliveryProtocol::TEnum ConfigValResultDeliveryProtocol::parse(string const& str) {
    // Using BOOST's 'iequals' for case-insensitive comparisons.
    if (str.empty() || boost::iequals(str, "HTTP")) {
        return HTTP;
    } else if (boost::iequals(str, "HTTP")) {
        return HTTP;
    } else if (boost::iequals(str, "XROOT")) {
        return XROOT;
    }
    throw ConfigException(ERR_LOC,
                          "WorkerConfig::" + string(__func__) + " unsupported method '" + str + "'.");
}

string ConfigValResultDeliveryProtocol::toString(TEnum protocol) {
    switch (protocol) {
        case HTTP:
            return "HTTP";
        case XROOT:
            return "XROOT";
    }
    throw ConfigException(ERR_LOC, "WorkerConfig::" + string(__func__) + ": unknown protocol " +
                                           to_string(static_cast<int>(protocol)));
}

void ConfigValMap::addEntry(ConfigVal::Ptr const& newVal) {
    std::string section = newVal->getSection();
    std::string name = newVal->getName();
    auto& nameMap = _sectionMap[section];
    auto iter = nameMap.find(name);
    if (iter != nameMap.end()) {
        throw ConfigException(ERR_LOC, "ConfigValMap already has entry for " + newVal->getSectionName());
    }
    nameMap[name] = newVal;
}

ConfigVal::Ptr ConfigValMap::getEntry(std::string const& section, std::string const& name) {
    auto iterSec = _sectionMap.find(section);
    if (iterSec == _sectionMap.end()) {
        return nullptr;
    }
    NameMap& nMap = iterSec->second;
    auto iterName = nMap.find(name);
    if (iterName == nMap.end()) {
        return nullptr;
    }
    return iterName->second;
}

void ConfigValMap::readConfigStore(util::ConfigStore const& configStore) {
    for (auto&& [section, nameMap] : _sectionMap) {
        for (auto&& [name, cfgVal] : nameMap) {
            try {
                if (auto cInt = dynamic_pointer_cast<ConfigValTInt>(cfgVal); cInt != nullptr) {
                    int64_t intVal = configStore.getIntRequired(cfgVal->getSectionName());
                    cInt->setVal(intVal);
                } else if (auto cUInt = dynamic_pointer_cast<ConfigValTUInt>(cfgVal); cUInt != nullptr) {
                    uint64_t uintVal = configStore.getIntRequired(cfgVal->getSectionName());
                    cUInt->setVal(uintVal);
                } else if (auto cStr = dynamic_pointer_cast<ConfigValTStr>(cfgVal); cStr != nullptr) {
                    string strVal = configStore.getRequired(cfgVal->getSectionName());
                    cStr->setVal(strVal);
                } else if (auto cCvrdp = dynamic_pointer_cast<ConfigValResultDeliveryProtocol>(cfgVal);
                           cCvrdp != nullptr) {
                    string strVal = configStore.getRequired(cfgVal->getSectionName());
                    auto cvrdpVal = ConfigValResultDeliveryProtocol::parse(strVal);
                    cCvrdp->setVal(cvrdpVal);
                } else {
                    throw ConfigException(
                            ERR_LOC, string(__func__) + " un-handled type for " + cfgVal->getSectionName());
                }
                cfgVal->setValSetFromFile(true);
            } catch (util::KeyNotFoundError const& e) {
                LOGS(_log, LOG_LVL_WARN,
                     " ConfigVal " << cfgVal->getSectionName() << " using default=" << cfgVal->getValStr());
            }
        }
    }
}

std::tuple<bool, std::string> ConfigValMap::checkRequired() const {
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& ConfigValMap::checkRequired() a");
    bool errorFound = false;
    string eMsg;
    for (auto&& [section, nameMap] : _sectionMap) {
        for (auto&& [name, cfgVal] : nameMap) {
            if (cfgVal->isRequired() && !cfgVal->isValSetFromFile()) {
                errorFound = true;
                eMsg += " " + cfgVal->getSectionName();
            }
        }
    }
    return {errorFound, eMsg};
}

shared_ptr<WorkerConfig> WorkerConfig::create(string const& configFileName) {
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& WorkerConfig::create a " << configFileName);
    lock_guard<mutex> const lock(_mtxOnInstance);
    if (_instance == nullptr) {
        _instance = shared_ptr<WorkerConfig>(configFileName.empty()
                                                     ? new WorkerConfig()
                                                     : new WorkerConfig(util::ConfigStore(configFileName)));
    }
    return _instance;
}

shared_ptr<WorkerConfig> WorkerConfig::instance() {
    lock_guard<mutex> const lock(_mtxOnInstance);
    if (_instance == nullptr) {
        throw logic_error("WorkerConfig::" + string(__func__) + ": instance has not been created.");
    }
    return _instance;
}

WorkerConfig::WorkerConfig()
        : _jsonConfig(nlohmann::json::object(
                  {{"input", nlohmann::json::object()}, {"actual", nlohmann::json::object()}})) {
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& WorkerConfig::WorkerConfig() a1 ");
    // Both collections are the same since we don't have any external configuration
    // source passed into this c-tor.
    _populateJsonConfig("input");
    _populateJsonConfig("actual");
}

WorkerConfig::WorkerConfig(const util::ConfigStore& configStore)
        : _jsonConfig(nlohmann::json::object(
                  {{"input", configStore.toJson()}, {"actual", nlohmann::json::object()}})) {
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& WorkerConfig::WorkerConfig() a2 ");

    _configValMap.readConfigStore(configStore);
    auto [errorFound, eMsg] = _configValMap.checkRequired();
    if (errorFound) {
        throw ConfigException(ERR_LOC, "worker config missing required value(s) " + eMsg);
    }

    if (_mysqlPort->getVal() == 0 && _mysqlSocket->getVal().empty()) {
        throw ConfigException(
                ERR_LOC, "At least one of mysql.port or mysql.socket is required in the configuration file.");
    }

    _mySqlConfig = mysql::MySqlConfig(_mysqlUsername->getVal(), _mysqlPassword->getVal(),
                                      _mysqlHostname->getVal(), _mysqlPort->getVal(), _mysqlSocket->getVal(),
                                      "");  // dbname

    _replicationAuthKey->setHidden();
    _replicationAdminAuthKey->setHidden();
    _mysqlPassword->setHidden();

    if (_replicationRegistryHost->getVal().empty()) {
        throw ConfigException(
                ERR_LOC, "WorkerConfig::" + string(__func__) + ": 'replication.registry_host' is not set.");
    }
    if (_replicationRegistryPort->getVal() == 0) {
        throw ConfigException(ERR_LOC, "WorkerConfig::" + string(__func__) +
                                               ": 'replication.registry_port' number can't be 0.");
    }
    if (_replicationRegistryHearbeatIvalSec->getVal() == 0) {
        throw ConfigException(ERR_LOC, "WorkerConfig::" + string(__func__) +
                                               ": 'replication.registry_heartbeat_ival_sec' can't be 0.");
    }
    if (_replicationNumHttpThreads->getVal() == 0) {
        throw ConfigException(ERR_LOC, "WorkerConfig::" + string(__func__) +
                                               ": 'replication.num_http_threads' can't be 0.");
    }

    // Note that actual collection may contain parameters not mentioned in
    // the input configuration.
    _populateJsonConfig("actual");
}

void WorkerConfig::setReplicationHttpPort(uint16_t port) {
    if (port == 0) {
        throw invalid_argument("WorkerConfig::" + string(__func__) + ": port number can't be 0.");
    }
    _replicationHttpPort->setVal(port);
    // Update the relevant section of the JSON-ified configuration.
    _jsonConfig["actual"]["replication"]["http_port"] = _replicationHttpPort->getValStr();
}

void WorkerConfig::_populateJsonConfig(string const& coll) {  //&&&
    nlohmann::json& jsonConfigCollection = _jsonConfig[coll];
    jsonConfigCollection["memman"] = nlohmann::json::object({{"class", _memManClass->getValStr()},
                                                             {"memory", _memManSizeMb->getValStr()},
                                                             {"location", _memManLocation->getValStr()}});
    jsonConfigCollection["scheduler"] =
            nlohmann::json::object({{"thread_pool_size", _threadPoolSize->getValStr()},
                                    {"max_pool_threads", _maxPoolThreads->getValStr()},
                                    {"group_size", _maxGroupSize->getValStr()},
                                    {"required_tasks_completed", _requiredTasksCompleted->getValStr()},
                                    {"priority_slow", _prioritySlow->getValStr()},
                                    {"priority_snail", _prioritySnail->getValStr()},
                                    {"priority_med", _priorityMed->getValStr()},
                                    {"priority_fast", _priorityFast->getValStr()},
                                    {"reserve_slow", _maxReserveSlow->getValStr()},
                                    {"reserve_snail", _maxReserveSnail->getValStr()},
                                    {"reserve_med", _maxReserveMed->getValStr()},
                                    {"reserve_fast", _maxReserveFast->getValStr()},
                                    {"maxactivechunks_slow", _maxActiveChunksSlow->getValStr()},
                                    {"maxactivechunks_snail", _maxActiveChunksSnail->getValStr()},
                                    {"maxactivechunks_med", _maxActiveChunksMed->getValStr()},
                                    {"maxactivechunks_fast", _maxActiveChunksFast->getValStr()},
                                    {"scanmaxminutes_fast", _scanMaxMinutesFast->getValStr()},
                                    {"scanmaxminutes_med", _scanMaxMinutesMed->getValStr()},
                                    {"scanmaxminutes_slow", _scanMaxMinutesSlow->getValStr()},
                                    {"scanmaxminutes_snail", _scanMaxMinutesSnail->getValStr()},
                                    {"maxtasksbootedperuserquery", _maxTasksBootedPerUserQuery->getValStr()},
                                    {"maxConcurrentBootedTasks", _maxConcurrentBootedTasks->getValStr()}});
    jsonConfigCollection["sqlconnections"] = nlohmann::json::object(
            {{"maxsqlconn", _maxSqlConnections->getValStr()},
             {"reservedinteractivesqlconn", _ReservedInteractiveSqlConnections->getValStr()}});
    jsonConfigCollection["transmit"] =
            nlohmann::json::object({{"buffermaxtotalgb", _bufferMaxTotalGB->getValStr()},
                                    {"maxtransmits", _maxTransmits->getValStr()},
                                    {"maxperqid", _maxPerQid->getValStr()}});
    jsonConfigCollection["results"] =
            nlohmann::json::object({{"dirname", _resultsDirname->getValStr()},
                                    {"xrootd_port", _resultsXrootdPort->getValStr()},
                                    {"num_http_threads", _resultsNumHttpThreads->getValStr()},
                                    {"protocol", _resultDeliveryProtocol->getValStr()},
                                    {"clean_up_on_start", _resultsCleanUpOnStart->getVal() ? "1" : "0"}});
    jsonConfigCollection["mysql"] = nlohmann::json::object({{"username", _mySqlConfig.username},
                                                            {"password", "xxxxx"},
                                                            {"hostname", _mySqlConfig.hostname},
                                                            {"port", to_string(_mySqlConfig.port)},
                                                            {"socket", _mySqlConfig.socket},
                                                            {"db", _mySqlConfig.dbName}});
    jsonConfigCollection["replication"] = nlohmann::json::object(
            {{"instance_id", _replicationInstanceId->getValStr()},
             {"auth_key", _replicationAuthKey->getValStr()},
             {"admin_auth_key", _replicationAdminAuthKey->getValStr()},
             {"registry_host", _replicationRegistryHost->getValStr()},
             {"registry_port", _replicationRegistryPort->getValStr()},
             {"registry_heartbeat_ival_sec", _replicationRegistryHearbeatIvalSec->getValStr()},
             {"http_port", _replicationHttpPort->getValStr()},
             {"num_http_threads", _replicationNumHttpThreads->getValStr()}});
}

void ConfigValMap::populateJson(nlohmann::json& js, string const& coll) {}

void WorkerConfig::_populateJsonConfigNew(string const& coll) {  //&&&
    nlohmann::json& jsonConfigCollection = _jsonConfig[coll];
    jsonConfigCollection["memman"] = nlohmann::json::object({{"class", _memManClass->getValStr()},
                                                             {"memory", _memManSizeMb->getValStr()},
                                                             {"location", _memManLocation->getValStr()}});
    jsonConfigCollection["scheduler"] =
            nlohmann::json::object({{"thread_pool_size", _threadPoolSize->getValStr()},
                                    {"max_pool_threads", _maxPoolThreads->getValStr()},
                                    {"group_size", _maxGroupSize->getValStr()},
                                    {"required_tasks_completed", _requiredTasksCompleted->getValStr()},
                                    {"priority_slow", _prioritySlow->getValStr()},
                                    {"priority_snail", _prioritySnail->getValStr()},
                                    {"priority_med", _priorityMed->getValStr()},
                                    {"priority_fast", _priorityFast->getValStr()},
                                    {"reserve_slow", _maxReserveSlow->getValStr()},
                                    {"reserve_snail", _maxReserveSnail->getValStr()},
                                    {"reserve_med", _maxReserveMed->getValStr()},
                                    {"reserve_fast", _maxReserveFast->getValStr()},
                                    {"maxactivechunks_slow", _maxActiveChunksSlow->getValStr()},
                                    {"maxactivechunks_snail", _maxActiveChunksSnail->getValStr()},
                                    {"maxactivechunks_med", _maxActiveChunksMed->getValStr()},
                                    {"maxactivechunks_fast", _maxActiveChunksFast->getValStr()},
                                    {"scanmaxminutes_fast", _scanMaxMinutesFast->getValStr()},
                                    {"scanmaxminutes_med", _scanMaxMinutesMed->getValStr()},
                                    {"scanmaxminutes_slow", _scanMaxMinutesSlow->getValStr()},
                                    {"scanmaxminutes_snail", _scanMaxMinutesSnail->getValStr()},
                                    {"maxtasksbootedperuserquery", _maxTasksBootedPerUserQuery->getValStr()},
                                    {"maxConcurrentBootedTasks", _maxConcurrentBootedTasks->getValStr()}});
    jsonConfigCollection["sqlconnections"] = nlohmann::json::object(
            {{"maxsqlconn", _maxSqlConnections->getValStr()},
             {"reservedinteractivesqlconn", _ReservedInteractiveSqlConnections->getValStr()}});
    jsonConfigCollection["transmit"] =
            nlohmann::json::object({{"buffermaxtotalgb", _bufferMaxTotalGB->getValStr()},
                                    {"maxtransmits", _maxTransmits->getValStr()},
                                    {"maxperqid", _maxPerQid->getValStr()}});
    jsonConfigCollection["results"] =
            nlohmann::json::object({{"dirname", _resultsDirname->getValStr()},
                                    {"xrootd_port", _resultsXrootdPort->getValStr()},
                                    {"num_http_threads", _resultsNumHttpThreads->getValStr()},
                                    {"protocol", _resultDeliveryProtocol->getValStr()},
                                    {"clean_up_on_start", _resultsCleanUpOnStart->getVal() ? "1" : "0"}});
    jsonConfigCollection["mysql"] = nlohmann::json::object({{"username", _mySqlConfig.username},
                                                            {"password", "xxxxx"},
                                                            {"hostname", _mySqlConfig.hostname},
                                                            {"port", to_string(_mySqlConfig.port)},
                                                            {"socket", _mySqlConfig.socket},
                                                            {"db", _mySqlConfig.dbName}});
    jsonConfigCollection["replication"] = nlohmann::json::object(
            {{"instance_id", _replicationInstanceId->getValStr()},
             {"auth_key", _replicationAuthKey->getValStr()},
             {"admin_auth_key", _replicationAdminAuthKey->getValStr()},
             {"registry_host", _replicationRegistryHost->getValStr()},
             {"registry_port", _replicationRegistryPort->getValStr()},
             {"registry_heartbeat_ival_sec", _replicationRegistryHearbeatIvalSec->getValStr()},
             {"http_port", _replicationHttpPort->getValStr()},
             {"num_http_threads", _replicationNumHttpThreads->getValStr()}});
}

ostream& operator<<(ostream& out, WorkerConfig const& workerConfig) {
    out << workerConfig._jsonConfig.dump();
    return out;
}

}  // namespace lsst::qserv::wconfig
