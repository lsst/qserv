/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONFILE_H
#define LSST_QSERV_REPLICA_CONFIGURATIONFILE_H

// System headers
#include <string>

// Qserv headers
#include "replica/ConfigurationStore.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationFile loads configuration parameters from a file.
  *
  * The implementation of this class relies upon the basic parser
  * of the INI-style configuration files.
  */
class ConfigurationFile
    :   public ConfigurationStore {

public:

    // Default construction and copy semantics are prohibited

    ConfigurationFile() = delete;
    ConfigurationFile(ConfigurationFile const&) = delete;
    ConfigurationFile& operator=(ConfigurationFile const&) = delete;

    /**
     * Construct the object by reading the configuration
     * from the specified file.
     *
     * @param configFile - the name of a configuration file
     */
    explicit ConfigurationFile(std::string const& configFile);

    ~ConfigurationFile() final = default;

    /**
     * @see Configuration::prefix()
     */
    virtual std::string prefix() const final { return "file"; }

    /**
     * @see Configuration::configUrl()
     */
    std::string configUrl() const final {
        return prefix() + ":" + _configFile;
    }

private:

    /// The name of the configuration file
    std::string const _configFile;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONFILE_H
