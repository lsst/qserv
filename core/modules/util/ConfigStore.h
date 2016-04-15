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
/**
 * @file
 *
 * @ingroup util
 *
 * @brief Provide common configuration management framework
 *
 * Manage czar and worker (xrdssi plugin) configuration files
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#ifndef LSST_QSERV_UTIL_CONFIGSTORE_H_
#define LSST_QSERV_UTIL_CONFIGSTORE_H_

// System headers
#include <map>
#include <string>

// Qserv headers

namespace lsst {
namespace qserv {
namespace util {

/**
 *  Read, store and provide read-only access for a key-value list
 *
 *  Used to manage Qserv configuration parameters.
 *
 *  Parse an INI configuration file, identify required parameters and ignore
 *  others, analyze and store them inside private member variables, use default
 *  values for missing parameters, provide accessor for each of these variable.
 *  This class hide configuration complexity from other part of the code.
 *  All private member variables are related to Czar parameters and are immutables.
 *
 */
class ConfigStore {
public:

    /** Build a ConfigStore object from a configuration file
     *
     * @param configFilePath: path to Qserv configuration file
     */
    ConfigStore(std::string const& configFilePath)
        : _configMap(_parseIniFile(configFilePath)) {
    };

    /** Build a ConfigStore object from a map
     *
     * @param kvMap: key-value map
     */
    ConfigStore(std::map<std::string, std::string> const& kvMap)
    :   _configMap(kvMap) {
    };

    ConfigStore(ConfigStore const&) = delete;
    ConfigStore& operator=(ConfigStore const&) = delete;


    /** Overload output operator for current class
     *
     * @param out
     * @param config
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, ConfigStore const& config);

    /** Get value for a configuration key
     *
     * @param key configuration key
     * @return the string value for a key
     * @throw KeyNotFoundError if key is not found
     */
    std::string getString(std::string const& key) const;

    /** Get value for a configuration key or a default value if key is not found
     *
     * @param key configuration key
     * @params defaultValue
     * @return the string value for a key, defaulting to defaultValue
     */
    std::string getStringOrDefault(std::string const& key,
        std::string const& defaultValue = std::string()) const;

    /** Get value for a configuration key or a default value if key is not found
     *
     * @param key configuration key
     * @params defaultValue
     * @return the integer value for a key, defaulting to defaultValue
     *
     * @throw InvalidIntegerValue if value can not be converted to an integer
     */
    int getIntOrDefault(std::string const& key, int const& defaultValue = 0) const;

    /** Get a collection of (key, value) related to a configuration section
     *
     *  All ConfigStore entries having key like "section.param_key" are returned
     *  but all key name are shortened to "param_key"
     *
     * @param sectionName name of configuration section
     * @return a collection of (key, value) related to a configuration section
     *
     */
    std::map<std::string, std::string> const getSectionConfigMap(std::string sectionName) const {
        // find all css.* parameters and copy to new map (dropping css.)
            std::string section = sectionName+".";
            std::map<std::string, std::string> sectionConfigMap;
            int len = section.length();
            for (auto const& kv : _configMap) {
                if (kv.first.compare(0, len, section) == 0) {
                    sectionConfigMap.insert(
                            std::make_pair(std::string(kv.first, 4), kv.second));
                }
            }
        return sectionConfigMap;
    }

private:

    static std::map<std::string, std::string> const  _parseIniFile(std::string const& configFilePath);

    std::map<std::string, std::string> const _configMap;

};

}}} // namespace lsst::qserv::util

#endif /* LSST_QSERV_UTIL_CONFIGSTORE_H_ */
