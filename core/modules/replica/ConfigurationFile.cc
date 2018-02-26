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

// Class header
#include "replica/ConfigurationFile.h"

// System headers
#include <boost/lexical_cast.hpp>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "util/ConfigStore.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationFile");

/**
 * Fetch and parse a value of the specified key into. Return the specified
 * default value if the parameter was not found.
 *
 * The function may throw the following exceptions:
 *
 *   std::bad_lexical_cast
 */
template <typename T, typename D>
void parseKeyVal (lsst::qserv::util::ConfigStore &configStore,
                  std::string const& key,
                  T&                 val,
                  D const&           defaultVal) {

    std::string const str = configStore.get(key);
    val = str.empty() ? defaultVal : boost::lexical_cast<T>(str);        
}

/**
 * Function specialization for the boolean type
 */
template <>
void parseKeyVal<bool,bool> (lsst::qserv::util::ConfigStore &configStore,
                             std::string const& key,
                             bool&              val,
                             bool const&        defaultVal) {

    unsigned int number;
    parseKeyVal (configStore, key, number, defaultVal ? 1 : 0);
    val = (bool) number;      
}

}  // namespace

namespace lsst {
namespace qserv {
namespace replica {

ConfigurationFile::ConfigurationFile (std::string const& configFile)
    :   Configuration (),
        _configFile (configFile) {

    loadConfiguration();
}

WorkerInfo const&
ConfigurationFile::disableWorker (std::string const& name) {

    std::string const context = "ConfigurationFile::disableWorker  ";

    LOGS(_log, LOG_LVL_ERROR, context << name);

    // This will also throw an exception if the worker is unknown
    WorkerInfo const& info = workerInfo(name);
    if (info.isEnabled) {
    
        // Then update the transient state (note this change will be also be)
        // seen via the above obtainer reference to the worker description.
        _workerInfo[info.name].isEnabled = false;
    }
    return info;
}

void
ConfigurationFile::deleteWorker (std::string const& name) {

    std::string const context = "ConfigurationFile::deleteWorker  ";

    LOGS(_log, LOG_LVL_ERROR, context << name);

    // This will also throw an exception if the worker is unknown
    WorkerInfo const& info = workerInfo(name);
    
    _workerInfo.erase(info.name);
}

void
ConfigurationFile::loadConfiguration () {

    lsst::qserv::util::ConfigStore configStore(_configFile);

    // Parse the list of worker names

    std::vector<std::string> workers;
    {
        std::istringstream ss(configStore.getRequired("common.workers"));
        std::istream_iterator<std::string> begin(ss), end;
        workers = std::vector<std::string>(begin, end);
    }
    std::vector<std::string> databaseFamilies;
    {
        std::istringstream ss(configStore.getRequired("common.database_families"));
        std::istream_iterator<std::string> begin(ss), end;
        databaseFamilies = std::vector<std::string>(begin, end);
    }
    std::vector<std::string> databases;
    {
        std::istringstream ss(configStore.getRequired("common.databases"));
        std::istream_iterator<std::string> begin(ss), end;
        databases = std::vector<std::string>(begin, end);
    }

    ::parseKeyVal(configStore, "common.request_buf_size_bytes",     _requestBufferSizeBytes,       defaultRequestBufferSizeBytes);
    ::parseKeyVal(configStore, "common.request_retry_interval_sec", _retryTimeoutSec,              defaultRetryTimeoutSec);

    ::parseKeyVal(configStore, "common.database_technology", _databaseTechnology, defaultDatabaseTechnology);
    ::parseKeyVal(configStore, "common.database_host",       _databaseHost,       defaultDatabaseHost);
    ::parseKeyVal(configStore, "common.database_port",       _databasePort,       defaultDatabasePort);
    ::parseKeyVal(configStore, "common.database_user",       _databaseUser,       defaultDatabaseUser);
    ::parseKeyVal(configStore, "common.database_password",   _databasePassword,   defaultDatabasePassword);
    ::parseKeyVal(configStore, "common.database_name",       _databaseName,       defaultDatabaseName);

    ::parseKeyVal(configStore, "controller.http_server_port",       _controllerHttpPort,           defaultControllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads",    _controllerHttpThreads,        defaultControllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec",    _controllerRequestTimeoutSec,  defaultControllerRequestTimeoutSec);

    ::parseKeyVal(configStore, "worker.technology",                 _workerTechnology,             defaultWorkerTechnology);
    ::parseKeyVal(configStore, "worker.num_svc_processing_threads", _workerNumProcessingThreads,   defaultWorkerNumProcessingThreads);
    ::parseKeyVal(configStore, "worker.num_fs_processing_threads",  _workerNumFsProcessingThreads, defaultWorkerNumFsProcessingThreads);
    ::parseKeyVal(configStore, "worker.fs_buf_size_bytes",          _workerFsBufferSizeBytes,      defaultWorkerFsBufferSizeBytes);


    // Optional common parameters for workers

    uint16_t commonWorkerSvcPort;
    uint16_t commonWorkerFsPort;

    ::parseKeyVal(configStore, "worker.svc_port", commonWorkerSvcPort, defaultWorkerSvcPort);
    ::parseKeyVal(configStore, "worker.fs_port",  commonWorkerFsPort,  defaultWorkerFsPort);

    std::string commonDataDir;
    
    ::parseKeyVal(configStore, "worker.data_dir",    commonDataDir,    defaultDataDir);

    // Parse optional worker-specific configuraton sections. Assume default
    // or (previously parsed) common values if a whole secton or individual
    // parameters are missing.

    for (std::string const& name: workers) {

        std::string const section = "worker:" + name;
        if (_workerInfo.count(name))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() duplicate worker entry: '" +
                    name + "' in: [common] or ["+section+"], configuration file: " + _configFile);

        _workerInfo[name].name = name;

        ::parseKeyVal(configStore, section+".is_enabled",   _workerInfo[name].isEnabled,  true);
        ::parseKeyVal(configStore, section+".is_read_only", _workerInfo[name].isReadOnly, false);
        ::parseKeyVal(configStore, section+".svc_host",     _workerInfo[name].svcHost,    defaultWorkerSvcHost);
        ::parseKeyVal(configStore, section+".svc_port",     _workerInfo[name].svcPort,    commonWorkerSvcPort);
        ::parseKeyVal(configStore, section+".fs_host",      _workerInfo[name].fsHost,     defaultWorkerFsHost);
        ::parseKeyVal(configStore, section+".fs_port",      _workerInfo[name].fsPort,     commonWorkerFsPort);
        ::parseKeyVal(configStore, section+".data_dir",     _workerInfo[name].dataDir,    commonDataDir);

        Configuration::translateDataDir(_workerInfo[name].dataDir, name);
    }

    // Parse mandatory database family-specific configuraton sections

    for (std::string const& name: databaseFamilies) {
        std::string const section = "database_family:" + name;
        if (_replicationLevel.count(name))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() duplicate database family entry: '" +
                    name + "' in: [common] or ["+section+"], configuration file: " + _configFile);
        ::parseKeyVal(configStore, section+".min_replication_level",   _replicationLevel[name], defaultReplicationLevel);
        if (!_replicationLevel[name]) _replicationLevel[name] = defaultReplicationLevel;
    }

    // Parse mandatory database-specific configuraton sections

    for (std::string const& name: databases) {

        std::string const section = "database:" + name;
        if (_databaseInfo.count(name))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() duplicate database entry: '" +
                    name + "' in: [common] or ["+section+"], configuration file: " + _configFile);

        _databaseInfo[name].name = name;
        _databaseInfo[name].family = configStore.getRequired(section+".family");
        if (!_replicationLevel.count(_databaseInfo[name].family))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() unknown database family: '" +
                    _databaseInfo[name].family + "' in section ["+section+"], configuration file: " + _configFile);
        {
            std::istringstream ss(configStore.getRequired(section+".partitioned_tables"));
            std::istream_iterator<std::string> begin(ss), end;
            _databaseInfo[name].partitionedTables = std::vector<std::string>(begin, end);
        }
        {
            std::istringstream ss(configStore.getRequired(section+".regular_tables"));
            std::istream_iterator<std::string> begin(ss), end;
            _databaseInfo[name].regularTables = std::vector<std::string>(begin, end);
        }
    }
    dumpIntoLogger ();
}
    
}}} // namespace lsst::qserv::replica