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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H
#define LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H

/// ConfigurationStore.h declares:
///
/// class ConfigurationStore
/// (see individual class documentation for more information)

// System headers
#include <string>

// Qserv headers
#include "replica/Configuration.h"

// Forward declarations

namespace lsst {
namespace qserv {
namespace util {
class ConfigStore;
}}} // namespace lsst::qserv::util

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ConfigurationStore is a base class for a family of configuration
  * classes which are designed to load configuration parameters from a transient
  * configuraton store. 
  *
  * This class also:
  *
  *   - enforces a specific schema for key names found in the store
  *   - ensures all required parameters are found in the input store
  *   - sets default values for the optional parameters
  *   - caches parameters in memory
  */
class ConfigurationStore
    :   public Configuration {

public:

    // Default construction and copy semantics are prohibited

    ConfigurationStore() = delete;
    ConfigurationStore(ConfigurationStore const&) = delete;
    ConfigurationStore& operator=(ConfigurationStore const&) = delete;

    ~ConfigurationStore() override = default;

    /**
     * @see Configuration::disableWorker()
     */
    WorkerInfo const disableWorker(std::string const& name) final;

    /**
     * @see Configuration::deleteWorker()
     */
    void deleteWorker(std::string const& name) final;

    /**
     * @see Configuration::setWorkerSvcPort()
     */
    WorkerInfo const setWorkerSvcPort(std::string const& name,
                                      uint16_t port) final;

    /**
     * @see Configuration::setWorkerFsPort()
     */
    WorkerInfo const setWorkerFsPort(std::string const& name,
                                     uint16_t port) final;
protected:

    /**
     * Construct an object by reading the configuration from the input
     * configuration store.
     *
     * @param configStore - reference to a configuraiton store object
     *
     * @throw std::runtime_error if the input configuration is not consistent
     * with expectations of the application
     */
    explicit ConfigurationStore(util::ConfigStore const& configStore);

private:

    /**
     * Read and validae input configuration parameters from the specified 
     * store and initialize the object.
     *
     * @param configStore - reference to a configuraiton store object
     *
     * @throw std::runtime_error if the input configuration is not consistent
     * with expectations of the application
     */
    void loadConfiguration(util::ConfigStore const& configStore);
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONSTORE_H
