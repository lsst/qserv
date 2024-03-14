// -*- LSST-C++ -*-
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
#include "util/ConfigValMap.h"

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

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.ConfigValMap");
}  // namespace

namespace lsst::qserv::util {

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

void ConfigValMap::populateJson(nlohmann::json& js, string const& coll) {
    for (auto const& [section, nMap] : _sectionMap) {
        nlohmann::json jsNames;
        for (auto const& [name, cfgPtr] : nMap) {
            jsNames[cfgPtr->getName()] = cfgPtr->getValStr();
        }
        js[section] = jsNames;
    }
}

}  // namespace lsst::qserv::util
