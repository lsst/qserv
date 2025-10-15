// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "ConfigStore.h"

// System headers
#include <filesystem>
#include <sstream>

// Qserv headers
#include "global/stringUtil.h"
#include "util/ConfigStoreError.h"
#include "util/IterableFormatter.h"

// Third-party headers
#include "boost/lexical_cast.hpp"
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// LSST headers
#include "lsst/log/Log.h"

namespace fs = std::filesystem;

namespace {  // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.ConfigStore");

}  // namespace

namespace lsst::qserv::util {

std::map<std::string, std::string> const ConfigStore::_parseIniFile(std::string const& configFilePath) {
    // read it into a ptree
    boost::property_tree::ptree pt;
    boost::property_tree::ini_parser::read_ini(configFilePath, pt);

    std::map<std::string, std::string> configMap;
    const fs::path basePath = fs::path(configFilePath).parent_path();

    // flatten
    for (auto& sectionPair : pt) {
        auto& section = sectionPair.first;
        for (auto& itemPair : sectionPair.second) {
            auto& item = itemPair.first;
            auto value = interpolateFile(itemPair.second.data(), basePath);
            configMap.insert(std::make_pair(section + "." + item, value));
        }
    }

    return configMap;
}

std::string ConfigStore::getRequired(std::string const& key) const {
    std::map<std::string, std::string>::const_iterator i = _configMap.find(key);
    if (i != _configMap.end()) {
        return i->second;
    } else {
        LOGS(_log, LOG_LVL_WARN, "[" << key << "] does not exist in configuration");
        throw KeyNotFoundError(key);
    }
}

std::string ConfigStore::get(std::string const& key, std::string const& defaultValue) const {
    std::map<std::string, std::string>::const_iterator i = _configMap.find(key);
    if (i != _configMap.end()) {
        return i->second;
    } else {
        LOGS(_log, LOG_LVL_DEBUG,
             "[" << key << "] key not found, using default value: \"" << defaultValue << "\"");
        return defaultValue;
    }
}

int ConfigStore::getInt(std::string const& key, int const& defaultValue) const {
    try {
        return getIntRequired(key);
    } catch (util::KeyNotFoundError const& e) {
        LOGS(_log, LOG_LVL_DEBUG, "Returning default value: \"" << defaultValue << "\"");
    }
    return defaultValue;
}

int ConfigStore::getIntRequired(std::string const& key) const {
    std::map<std::string, std::string>::const_iterator i = _configMap.find(key);
    if (i != _configMap.end() and not i->second.empty()) {
        try {
            // tried to use std::stoi() here but it returns OK for strings like "0xFSCK"
            return boost::lexical_cast<int>(i->second);
        } catch (boost::bad_lexical_cast const& exc) {
            LOGS(_log, LOG_LVL_WARN, "Unable to cast string \"" << i->second << "\" to integer");
            throw InvalidIntegerValue(key, i->second);
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "[" << key << "] key does not exist or has empty string value");
    throw util::KeyNotFoundError(key);
}

std::set<std::string> ConfigStore::getSections() const {
    std::set<std::string> sections;
    for (auto [key, val] : _configMap) {
        if (auto const pos = key.find("."); pos != std::string::npos) {
            sections.insert(key.substr(0, pos));
        }
    }
    return sections;
}

std::map<std::string, std::string> ConfigStore::getSectionConfigMap(std::string sectionName) const {
    // Copy all parameters matching "<sectionName>.<param>" to new map (dropping "<sectionName>.")
    std::string const section = sectionName + ".";
    std::map<std::string, std::string> sectionConfigMap;
    int len = section.length();
    for (auto&& [key, val] : _configMap) {
        if (key.substr(0, len) == section) {
            sectionConfigMap[key.substr(len)] = val;
        }
    }
    return sectionConfigMap;
}

nlohmann::json ConfigStore::toJson(bool scramblePasswords) const {
    // This should scramble anything that contains "auth_key" or
    // starts with "passw".
    std::string const passwordBegin = "passw";
    nlohmann::json result = nlohmann::json::object();
    for (auto&& sect : this->getSections()) {
        result[sect] = nlohmann::json::object();
        nlohmann::json& sectJson = result[sect];
        for (auto [param, val] : this->getSectionConfigMap(sect)) {
            bool const scramble =
                    scramblePasswords && ((param.substr(0, passwordBegin.size()) == passwordBegin) ||
                                          (param.find("auth_key") != std::string::npos));
            sectJson[param] = scramble ? "xxxxx" : val;
        }
    }
    return result;
}

std::ostream& operator<<(std::ostream& out, ConfigStore const& config) {
    out << util::printable(config._configMap);
    return out;
}

}  // namespace lsst::qserv::util
