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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H
#define LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H

/**
 * This header defines helper types which are meant to reduce code duplications
 * in applications (command line tools and REST services) dealing with
 * the class Configuration.
 */

// System headers
#include <cstdint>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ConfigurationSchema.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace detail {

// Type conversion traits.

template <typename T>
struct ConfigParamHandlerTrait {
    static std::string to_string(T const& val) { return std::to_string(val); }
};

template <>
struct ConfigParamHandlerTrait<std::string> {
    static std::string to_string(std::string const& val) { return val; }
};

/**
 * The utility class ConfigParamHandler represents parameter value handlers.
 */
template <typename T>
class ConfigParamHandler {
public:
    /// The flag indicating if the transient value of the parameter can be saved back
    /// into the Configuration. This flag can be used by the command-line tools
    /// and Web UI applications.
    bool const updatable;

    /// The name of the parameter's category.
    std::string const category;

    /// The name of the parameter within its category.
    std::string const name;

    /// The full path name to a parameter and the name of a key to be used in various
    /// contexts when a text-based reference to the corresponding Configuration parameter
    /// is needed (within protocols and application's implementations).
    std::string const key;

    /// This variable stores a transient value of the parameter before forwarding
    /// it to the Configuration by method 'save' (if the one is enabled). This variable
    /// gets initialized with the same value as the one set in variable 'defaultValue'.
    T value;

    /// The default value to be compared with the one of variable 'value' to see if the change
    /// has to propagate to the Configuration when method 'save()' gets called and if
    /// updates are allowed.
    T const defaultValue;

    ConfigParamHandler(bool updatable_,
                       std::string const& category_,
                       std::string const& name_,
                       T const& defaultValue_=T())
        :   updatable(updatable_),
            category(category_),
            name(name_),
            key(category_ + "." + name_),
            value(defaultValue_),
            defaultValue(defaultValue_) {
    }

    /// @return An expanded human-readable description of the parameter, its role, etc.
    ///   Values returned by the method  are used in the command-line tools's --help options
    ///   and Web UI applications where parameters are presented to users.
    std::string description() const { return ConfigurationSchema::description(category, name); }

    /**
     * The type-aware method returning a value of the parameter retrieved from
     * the Configuration.
     * @param config A pointer to the Configuration API.
     * @return A value of the parameter retrieved from the Configuration.
     */
    T get(Configuration const& config) const {
        return config.get<T>(category, name);
    }

    /**
     * The method for converting a value of the parameter pulled from the Configuration
     * into a string.
     * @param config A pointer to the Configuration API.
     */
    std::string str(Configuration const& config) const {
        return ConfigParamHandlerTrait<T>::to_string(get(config));
    }

    /**
     * The method that pushes (if allowed by flag 'updateable') a value stored in
     * the variable 'value' to the Configuration.
     * @param config A pointer to the Configuration API.
     */
    void save(Configuration& config) {
        if (updatable && (value != defaultValue)) {
            config.set<T>(category, name, value);
        }
    }

    /**
     * @param config A pointer to the Configuration API.
     * @return The JSON representation for the parameter.
     */
    nlohmann::json toJson(Configuration const& config) const {
        return nlohmann::json({
            {"updatable", updatable ? 1 : 0},
            {"parameter", key},
            {"description", description()},
            {"value", get(config)}
        });
    }
};
}   // namespace: detail

/**
 * The class ConfigurationGeneralParams encapsulates handlers for the general parameters
 * of the Configuration system's API. The handlers are needed to eliminate code
 * duplication and streamline implementations in some command line tools and REST services
 * dealing with the Replication System's Configuration.
 * 
 * Each handler is represented by a dedicated class detail::ConfigParamHandler<T>
 * explained above. Some handlers don't allow updates to be made to the Configuration
 * via the handler's interface.
 * @see detail:ConfigParamHandler
 */
class ConfigurationGeneralParams {
public:
    detail::ConfigParamHandler<int> metaVersion =
            detail::ConfigParamHandler<int>(false, "meta", "version", 0);

    detail::ConfigParamHandler<size_t> requestBufferSizeBytes =
            detail::ConfigParamHandler<size_t>(true, "common", "request_buf_size_bytes", 0);

    detail::ConfigParamHandler<unsigned int> retryTimeoutSec =
            detail::ConfigParamHandler<unsigned int>(true, "common", "request_retry_interval_sec", 0);

    detail::ConfigParamHandler<size_t> controllerThreads =
            detail::ConfigParamHandler<size_t>(true, "controller", "num_threads", 0);

    detail::ConfigParamHandler<size_t> controllerHttpThreads =
            detail::ConfigParamHandler<size_t>(true, "controller", "http_server_threads", 0);

    detail::ConfigParamHandler<uint16_t> controllerHttpPort =
            detail::ConfigParamHandler<uint16_t>(true, "controller", "http_server_port", 0);

    detail::ConfigParamHandler<unsigned int> controllerRequestTimeoutSec =
            detail::ConfigParamHandler<unsigned int>(true, "controller", "request_timeout_sec", 0);

    detail::ConfigParamHandler<unsigned int> jobTimeoutSec =
            detail::ConfigParamHandler<unsigned int>(true, "controller", "job_timeout_sec", 0);

    detail::ConfigParamHandler<unsigned int> jobHeartbeatTimeoutSec =
            detail::ConfigParamHandler<unsigned int>(true, "controller", "job_heartbeat_sec",
                                                     std::numeric_limits<unsigned int>::max());

    detail::ConfigParamHandler<std::string> controllerEmptyChunksDir =
            detail::ConfigParamHandler<std::string>(true, "controller", "empty_chunks_dir");

    detail::ConfigParamHandler<size_t> databaseServicesPoolSize =
            detail::ConfigParamHandler<size_t>(true, "database", "services_pool_size", 0);

    detail::ConfigParamHandler<std::string> databaseHost =
            detail::ConfigParamHandler<std::string>(false, "database", "host");

    detail::ConfigParamHandler<uint16_t> databasePort =
            detail::ConfigParamHandler<uint16_t>(false, "database", "port", 0);

    detail::ConfigParamHandler<std::string> databaseUser =
            detail::ConfigParamHandler<std::string>(false, "database", "user");

    detail::ConfigParamHandler<std::string> databaseName =
            detail::ConfigParamHandler<std::string>(false, "database", "name");

    detail::ConfigParamHandler<std::string> qservMasterDatabaseHost =
            detail::ConfigParamHandler<std::string>(true, "database", "qserv_master_host");

    detail::ConfigParamHandler<uint16_t> qservMasterDatabasePort =
            detail::ConfigParamHandler<uint16_t>(true, "database", "qserv_master_port", 0);

    detail::ConfigParamHandler<std::string> qservMasterDatabaseUser =
            detail::ConfigParamHandler<std::string>(true, "database", "qserv_master_user");

    detail::ConfigParamHandler<std::string> qservMasterDatabaseName =
            detail::ConfigParamHandler<std::string>(true, "database", "qserv_master_name");

    detail::ConfigParamHandler<size_t> qservMasterDatabaseServicesPoolSize =
            detail::ConfigParamHandler<size_t>(true, "database", "qserv_master_services_pool_size", 0);

    detail::ConfigParamHandler<std::string> qservMasterDatabaseTmpDir =
            detail::ConfigParamHandler<std::string>(true, "database", "qserv_master_tmp_dir");

    detail::ConfigParamHandler<int> xrootdAutoNotify =
            detail::ConfigParamHandler<int>(true, "xrootd", "auto_notify", -1);

    detail::ConfigParamHandler<unsigned int> xrootdTimeoutSec =
            detail::ConfigParamHandler<unsigned int>(true, "xrootd", "request_timeout_sec", 0);

    detail::ConfigParamHandler<std::string> xrootdHost =
            detail::ConfigParamHandler<std::string>(true, "xrootd", "host");

    detail::ConfigParamHandler<uint16_t> xrootdPort =
            detail::ConfigParamHandler<uint16_t>(true, "xrootd", "port", 0);

    detail::ConfigParamHandler<std::string> workerTechnology =
            detail::ConfigParamHandler<std::string>(true, "worker", "technology");

    detail::ConfigParamHandler<size_t> workerNumProcessingThreads =
            detail::ConfigParamHandler<size_t>(true, "worker", "num_svc_processing_threads", 0);

    detail::ConfigParamHandler<size_t> fsNumProcessingThreads =
            detail::ConfigParamHandler<size_t>(true, "worker", "num_fs_processing_threads", 0);

    detail::ConfigParamHandler<size_t> workerFsBufferSizeBytes =
            detail::ConfigParamHandler<size_t>(true, "worker", "fs_buf_size_bytes", 0);

    detail::ConfigParamHandler<size_t> loaderNumProcessingThreads =
            detail::ConfigParamHandler<size_t>(true, "worker", "num_loader_processing_threads", 0);
    
    detail::ConfigParamHandler<size_t> exporterNumProcessingThreads =
            detail::ConfigParamHandler<size_t>(true, "worker", "num_exporter_processing_threads", 0);
    
    detail::ConfigParamHandler<size_t> httpLoaderNumProcessingThreads =
            detail::ConfigParamHandler<size_t>(true, "worker", "num_http_loader_processing_threads", 0);

    detail::ConfigParamHandler<uint16_t> workerDefaultSvcPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "svc_port", 0);

    detail::ConfigParamHandler<uint16_t> workerDefaultFsPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "fs_port", 0);

    detail::ConfigParamHandler<std::string> workerDefaultDataDir =
            detail::ConfigParamHandler<std::string>(true, "worker_defaults", "data_dir");

    detail::ConfigParamHandler<uint16_t> workerDefaultDbPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "db_port", 0);

    detail::ConfigParamHandler<std::string> workerDefaultDbUser =
            detail::ConfigParamHandler<std::string>(true, "worker_defaults", "db_user");

    detail::ConfigParamHandler<uint16_t> workerDefaultLoaderPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "loader_port", 0);

    detail::ConfigParamHandler<std::string> workerDefaultLoaderTmpDir =
            detail::ConfigParamHandler<std::string>(true, "worker_defaults", "loader_tmp_dir");

    detail::ConfigParamHandler<uint16_t> workerDefaultExporterPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "exporter_port", 0);

    detail::ConfigParamHandler<std::string> workerDefaultExporterTmpDir =
            detail::ConfigParamHandler<std::string>(true, "worker_defaults", "exporter_tmp_dir");

    detail::ConfigParamHandler<uint16_t> workerDefaultHttpLoaderPort =
            detail::ConfigParamHandler<uint16_t>(true, "worker_defaults", "http_loader_port", 0);

    detail::ConfigParamHandler<std::string> workerDefaultHttpLoaderTmpDir =
            detail::ConfigParamHandler<std::string>(true, "worker_defaults", "http_loader_tmp_dir");

    /**
     * Pull general parameters from the Configuration and put them into a JSON array.
     *
     * @param config A pointer to the Configuration object.
     * @return The JSON array representing parameters.
     */
    nlohmann::json toJson(Configuration const& config) const;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H
