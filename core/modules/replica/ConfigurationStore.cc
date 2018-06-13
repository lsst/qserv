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

// Class header
#include "replica/ConfigurationStore.h"

// System headers
#include <iterator>
#include <sstream>
#include <stdexcept>

// Third party headers
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ChunkNumber.h"
#include "util/ConfigStore.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationStore");

using namespace lsst::qserv;

/**
 * Fetch and parse a value of the specified key into. Return the specified
 * default value if the parameter was not found.
 *
 * The function may throw the following exceptions:
 *
 *   std::bad_lexical_cast
 */
template<typename T, typename D>
void parseKeyVal(util::ConfigStore const& configStore,
                 std::string const& key,
                 T& val,
                 D const& defaultVal) {

    std::string const str = configStore.get(key);
    val = str.empty() ? defaultVal : boost::lexical_cast<T>(str);
}

/**
 * Function specialization for the boolean type
 */
template<>
void parseKeyVal<bool,bool>(util::ConfigStore const& configStore,
                            std::string const& key,
                            bool& val,
                            bool const& defaultVal) {

    unsigned int number;
    parseKeyVal(configStore, key, number, defaultVal ? 1 : 0);
    val = (bool) number;
}

}  // namespace

namespace lsst {
namespace qserv {
namespace replica {

ConfigurationStore::ConfigurationStore(util::ConfigStore const& configStore)
    :   Configuration() {
    loadConfiguration(configStore);
}

WorkerInfo const ConfigurationStore::disableWorker(std::string const& name) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "disableWorker  name=" << name);

    util::Lock lock(_mtx, context() + "disableWorker");

    auto&& itr = _workerInfo.find(name);
    if (_workerInfo.end() == itr) {
        throw std::invalid_argument("ConfigurationStore::disableWorker  no such worker: " + name);
    }
    itr->second.isEnabled = false;

    return itr->second;
}

void ConfigurationStore::deleteWorker(std::string const& name) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "deleteWorker  name=" << name);

    util::Lock lock(_mtx, context() + "deleteWorker");

    auto&& itr = _workerInfo.find(name);
    if (_workerInfo.end() == itr) {
        throw std::invalid_argument("ConfigurationStore::deleteWorker  no such worker: " + name);
    }
    _workerInfo.erase(itr);
}

WorkerInfo const ConfigurationStore::setWorkerSvcPort(std::string const& name,
                                                      uint16_t port) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setWorkerSvcPort  name=" << name << " port=" << port);

    util::Lock lock(_mtx, context() + "setWorkerSvcPort");

    auto&& itr = _workerInfo.find(name);
    if (_workerInfo.end() == itr) {
        throw std::invalid_argument("ConfigurationStore::setWorkerSvcPort  no such worker: " + name);
    }
    itr->second.svcPort = port;

    return itr->second;
}

WorkerInfo const ConfigurationStore::setWorkerFsPort(std::string const& name,
                                                     uint16_t port) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setWorkerFsPort  name=" << name << " port=" << port);

    util::Lock lock(_mtx, context() + "setWorkerFsPort");

    auto&& itr = _workerInfo.find(name);
    if (_workerInfo.end() == itr) {
        throw std::invalid_argument("ConfigurationStore::setWorkerFsPort  no such worker: " + name);
    }
    itr->second.fsPort = port;

    return itr->second;}

void ConfigurationStore::loadConfiguration(util::ConfigStore const& configStore) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "ConfigurationStore::loadConfiguration");

    util::Lock lock(_mtx, context() + "ConfigurationStore::loadConfiguration");

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

    ::parseKeyVal(configStore, "controller.num_threads",         _controllerThreads,           defaultControllerThreads);
    ::parseKeyVal(configStore, "controller.http_server_port",    _controllerHttpPort,          defaultControllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads", _controllerHttpThreads,       defaultControllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec", _controllerRequestTimeoutSec, defaultControllerRequestTimeoutSec);
    ::parseKeyVal(configStore, "controller.job_timeout_sec",     _jobTimeoutSec,               defaultJobTimeoutSec);
    ::parseKeyVal(configStore, "controller.job_heartbeat_sec",   _jobHeartbeatTimeoutSec,      defaultJobHeartbeatTimeoutSec);

    ::parseKeyVal(configStore, "xrootd.auto_notify",         _xrootdAutoNotify, defaultXrootdAutoNotify);
    ::parseKeyVal(configStore, "xrootd.host",                _xrootdHost,       defaultXrootdHost);
    ::parseKeyVal(configStore, "xrootd.port",                _xrootdPort,       defaultXrootdPort);
    ::parseKeyVal(configStore, "xrootd.request_timeout_sec", _xrootdTimeoutSec, defaultXrootdTimeoutSec);

    ::parseKeyVal(configStore, "worker.technology",                 _workerTechnology,             defaultWorkerTechnology);
    ::parseKeyVal(configStore, "worker.num_svc_processing_threads", _workerNumProcessingThreads,   defaultWorkerNumProcessingThreads);
    ::parseKeyVal(configStore, "worker.num_fs_processing_threads",  _fsNumProcessingThreads,       defaultFsNumProcessingThreads);
    ::parseKeyVal(configStore, "worker.fs_buf_size_bytes",          _workerFsBufferSizeBytes,      defaultWorkerFsBufferSizeBytes);


    // Optional common parameters for workers

    uint16_t commonWorkerSvcPort;
    uint16_t commonWorkerFsPort;

    ::parseKeyVal(configStore, "worker.svc_port", commonWorkerSvcPort, defaultWorkerSvcPort);
    ::parseKeyVal(configStore, "worker.fs_port",  commonWorkerFsPort,  defaultWorkerFsPort);

    std::string commonDataDir;

    ::parseKeyVal(configStore, "worker.data_dir",  commonDataDir, defaultDataDir);

    // Parse optional worker-specific configuraton sections. Assume default
    // or (previously parsed) common values if a whole secton or individual
    // parameters are missing.

    for (std::string const& name: workers) {

        std::string const section = "worker:" + name;
        if (_workerInfo.count(name)) {
            throw std::range_error(
                    "ConfigurationStore::loadConfiguration() duplicate worker entry: '" +
                    name + "' in: [common] or ["+section+"]");
        }
        auto& workerInfo = _workerInfo[name];
        workerInfo.name = name;

        ::parseKeyVal(configStore, section+".is_enabled",   workerInfo.isEnabled,  true);
        ::parseKeyVal(configStore, section+".is_read_only", workerInfo.isReadOnly, false);
        ::parseKeyVal(configStore, section+".svc_host",     workerInfo.svcHost,    defaultWorkerSvcHost);
        ::parseKeyVal(configStore, section+".svc_port",     workerInfo.svcPort,    commonWorkerSvcPort);
        ::parseKeyVal(configStore, section+".fs_host",      workerInfo.fsHost,     defaultWorkerFsHost);
        ::parseKeyVal(configStore, section+".fs_port",      workerInfo.fsPort,     commonWorkerFsPort);
        ::parseKeyVal(configStore, section+".data_dir",     workerInfo.dataDir,    commonDataDir);

        Configuration::translateDataDir(workerInfo.dataDir, name);
    }

    // Parse mandatory database family-specific configuraton sections

    for (std::string const& name: databaseFamilies) {
        std::string const section = "database_family:" + name;
        if (_databaseFamilyInfo.count(name)) {
            throw std::range_error(
                    "ConfigurationStore::loadConfiguration() duplicate database family entry: '" +
                    name + "' in: [common] or ["+section+"]");
        }
        _databaseFamilyInfo[name].name = name;
        ::parseKeyVal(configStore, section+".min_replication_level", _databaseFamilyInfo[name].replicationLevel, defaultReplicationLevel);
        if (not _databaseFamilyInfo[name].replicationLevel) {
            _databaseFamilyInfo[name].replicationLevel= defaultReplicationLevel;
        }
        ::parseKeyVal(configStore, section+".num_stripes", _databaseFamilyInfo[name].numStripes, defaultNumStripes);
        if (not _databaseFamilyInfo[name].numStripes) {
            _databaseFamilyInfo[name].numStripes= defaultNumStripes;
        }
        ::parseKeyVal(configStore, section+".num_sub_stripes", _databaseFamilyInfo[name].numSubStripes, defaultNumSubStripes);
        if (not _databaseFamilyInfo[name].numSubStripes) {
            _databaseFamilyInfo[name].numSubStripes= defaultNumSubStripes;
        }
        _databaseFamilyInfo[name].chunkNumberValidator =
            std::make_shared<ChunkNumberQservValidator>(
                    static_cast<int32_t>(_databaseFamilyInfo[name].numStripes),
                    static_cast<int32_t>(_databaseFamilyInfo[name].numSubStripes));
    }

    // Parse mandatory database-specific configuraton sections

    for (std::string const& name: databases) {

        std::string const section = "database:" + name;
        if (_databaseInfo.count(name)) {
            throw std::range_error(
                    "ConfigurationStore::loadConfiguration() duplicate database entry: '" +
                    name + "' in: [common] or ["+section+"]");
        }
        _databaseInfo[name].name = name;
        _databaseInfo[name].family = configStore.getRequired(section+".family");
        if (not _databaseFamilyInfo.count(_databaseInfo[name].family)) {
            throw std::range_error(
                    "ConfigurationStore::loadConfiguration() unknown database family: '" +
                    _databaseInfo[name].family + "' in section ["+section+"]");
        }
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
    dumpIntoLogger();
}

}}} // namespace lsst::qserv::replica
