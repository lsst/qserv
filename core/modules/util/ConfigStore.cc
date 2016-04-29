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

#include <sstream>

// Qserv headers
#include "util/ConfigStoreError.h"
#include "util/IterableFormatter.h"

// Third-party headers
#include "boost/lexical_cast.hpp"
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// LSST headers
#include "lsst/log/Log.h"


namespace { // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.Config");

} // namespace


namespace lsst {
namespace qserv {
namespace util {


void ConfigStore::parseFile(std::string const& configFilePath) {

    // read it into a ptree
    boost::property_tree::ptree pt;
    boost::property_tree::ini_parser::read_ini(configFilePath, pt);

    // flatten
    for (auto& sectionPair: pt) {
        auto& section = sectionPair.first;
        for (auto& itemPair: sectionPair.second) {
            auto& item = itemPair.first;
            auto& value = itemPair.second.data();
            _configMap.insert(std::make_pair(section + "." + item, value));
        }
    }
}

std::string ConfigStore::get(std::string const& key) const {
    StringMap::const_iterator i = _configMap.find(key);
    if(i != _configMap.end()) {
        return i->second;
    } else {
        LOGS( _log, LOG_LVL_WARN, "[" << key << "] does not exist in configuration");
        throw KeyNotFoundError(key);
    }
}

std::string ConfigStore::get(std::string const& key,
                             std::string const& defaultValue) const {
    StringMap::const_iterator i = _configMap.find(key);
    if(i != _configMap.end()) {
        return i->second;
    } else {
        LOGS( _log, LOG_LVL_WARN, "[" << key << "] key not found, using default value: \"" << defaultValue << "\"");
        return defaultValue;
    }
}

int ConfigStore::getInt(std::string const& key, int const& defaultValue) const {
    StringMap::const_iterator i = _configMap.find(key);
    if (i != _configMap.end()) {
        return boost::lexical_cast<int>(i->second);
    } else {
        LOGS( _log, LOG_LVL_WARN, "[" << key << "] key not found, using default value: \"" << defaultValue << "\"");
        return defaultValue;
    }
}


/** Overload output operator for this class
 *
 * @param out
 * @param config
 * @return an output stream
 */
std::ostream& operator<<(std::ostream &out, ConfigStore const& config) {
    out << util::printable(config._configMap);
    return out;
}

}}} // namespace lsst::qserv::util
