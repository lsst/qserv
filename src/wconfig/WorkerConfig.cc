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

void ConfigVal::addToMapBase(ConfigValMap& configValMap, Ptr const& newPtr) { configValMap.addEntry(newPtr); }

void ConfigVal::logValSet(std::string const& msg) {
    LOGS(_log, LOG_LVL_INFO, "ConfigVal " << getSectionName() << " set to " << getValStr() + " " + msg);
}

void ConfigVal::setValFromConfigStore(util::ConfigStore const& configStore) {
    try {
        setValFromConfigStoreChild(configStore);
        setValSetFromFile(true);
    } catch (util::KeyNotFoundError const& e) {
        LOGS(_log, LOG_LVL_WARN,
             " ConfigVal no entry for " << getSectionName() << " using default=" << getValStr());
    }
}

string ConfigValTBool::toString(bool val) {
    if (val == 0) {
        return string("false");
    }
    return string("true");
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
                cfgVal->setValFromConfigStore(configStore);
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

ConfigValResultDeliveryProtocol::TEnum ConfigValResultDeliveryProtocol::parse(string const& str) {
    // Convert to upper case for case-insensitive comparisons.
    string strUp;
    for (auto ch : str) {
        strUp += toupper(ch);
    }
    if (str.empty() || strUp == "HTTP") {
        return HTTP;
    } else if (strUp == "XROOT") {
        return XROOT;
    }
    throw ConfigException(ERR_LOC, string("ConfigValResultDeliveryProtocol::") + __func__ +
                                           " could not parse '" + str + "'.");
}

void ConfigValResultDeliveryProtocol::setValFromConfigStoreChild(util::ConfigStore const& configStore) {
    std::string str = configStore.getRequired(getSectionName());
    try {
        setVal(parse(str));
    } catch (ConfigException const& exc) {
        // Throw a similar exception with additional information.
        throw ConfigException(ERR_LOC, getSectionName() + " " + exc.what());
    }
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

void ConfigValMap::populateJson(nlohmann::json& js, string const& coll) {
    for (auto const& [section, nMap] : _sectionMap) {
        nlohmann::json jsNames;
        for (auto const& [name, cfgPtr] : nMap) {
            jsNames[cfgPtr->getName()] = cfgPtr->getValStr();
        }
        js[section] = jsNames;
    }
}

void WorkerConfig::_populateJsonConfig(string const& coll) {
    nlohmann::json& js = _jsonConfig[coll];
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& _populateJsonConfigNew b" << js);
    _configValMap.populateJson(js, coll);
    LOGS(_log, LOG_LVL_ERROR, __func__ << " &&& _populateJsonConfigNew b" << js);
}

ostream& operator<<(ostream& out, WorkerConfig const& workerConfig) {
    out << workerConfig._jsonConfig.dump();
    return out;
}

}  // namespace lsst::qserv::wconfig
