/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_CONFIGURATION_FILE_H
#define LSST_QSERV_REPLICA_CONFIGURATION_FILE_H

/// ConfigurationFile.h declares:
///
/// class ConfigurationFile
/// (see individual class documentation for more information)

// System headers
#include <string>

// Qserv headers
#include "replica/Configuration.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationFile loads configuration parameters from a file.
  *
  * The implementation of this class relies upon the basic parser
  * of the INI-style configuration files. In addition to the basic parser,
  * this class also:
  *
  *   - enforces a specific schema of the INI file
  *   - ensures all required parameters are found in the file
  *   - sets default values for the optional parameters
  *   - caches parameters in memory
  */
class ConfigurationFile
    :   public Configuration {

public:

    // Default construction and copy semantics are prohibited

    ConfigurationFile() = delete;
    ConfigurationFile(ConfigurationFile const&) = delete;
    ConfigurationFile& operator=(ConfigurationFile const&) = delete;

    /**
     * Construct the object by reading the configuration
     * from the specified file.
     *
     * @param configFile - the name of a configuraiton file
     */
    explicit ConfigurationFile(std::string const& configFile);

    /// Destructor
    ~ConfigurationFile() override = default;

    /**
     * Implements the method defined in the base class
     *
     * @see Configuration::configUrl ()
     */
    std::string configUrl() const override {
        return "file:" + _configFile;
    }

    /**
     * Implements the method defined in the base class
     *
     * @see Configuration::disableWorker ()
     */
    WorkerInfo const& disableWorker(std::string const& name) override;

    /**
     * Implements the method defined in the base class
     *
     * @see Configuration::deleteWorker ()
     */
    void deleteWorker(std::string const& name) override;

private:

    /**
     * Analyze the configuration and initialize the cache of parameters.
     *
     * The method will throw one of these exceptions:
     *
     *   std::runtime_error
     *      the configuration is not consistent with expectations of the application
     */
    void loadConfiguration();

private:

    /// The name of the configuration file
    std::string const _configFile;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATION_FILE_H
