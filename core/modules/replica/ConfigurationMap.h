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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONMAP_H
#define LSST_QSERV_REPLICA_CONFIGURATIONMAP_H

/**
 * This header defines a class which is used in an implementation
 * of the Configuration service.
 */

// System headers
#include <map>
#include <string>

// Qserv headers
#include "replica/ConfigurationStore.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationMap loads configuration parameters from a transient
  * key-value map as defined by class util::ConfigStore.
  */
class ConfigurationMap : public ConfigurationStore {
public:
    ConfigurationMap() = delete;
    ConfigurationMap(ConfigurationMap const&) = delete;
    ConfigurationMap& operator=(ConfigurationMap const&) = delete;

    /**
     * Initialize the configuration from a map.
     *
     * @param kvMap The key-value map with configuration parameters.
     * @see ConfigurationStore::ConfigurationStore()
     */
    explicit ConfigurationMap(std::map<std::string, std::string> const& kvMap);

    ~ConfigurationMap() final = default;

    std::string prefix() const final { return "map"; }

    std::string configUrl(bool showPassword=false) const final { return prefix() + ":"; }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONMAP_H
