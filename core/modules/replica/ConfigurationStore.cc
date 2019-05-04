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

// Class header
#include "replica/ConfigurationStore.h"

// System headers
#include <algorithm>
#include <iterator>
#include <sstream>

// Third party headers
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "replica/ChunkNumber.h"
#include "util/ConfigStore.h"

using namespace std;

namespace {

using namespace lsst::qserv;

/**
 * Fetch and parse a value of the specified key into. Return the specified
 * default value if the parameter was not found.
 *
 * @throw boost::bad_lexical_cast
 */
template<typename T, typename D>
void parseKeyVal(util::ConfigStore const& configStore,
                 string const& key,
                 T& val,
                 D const& defaultVal) {

    string const str = configStore.get(key);
    val = str.empty() ? defaultVal : boost::lexical_cast<T>(str);
}


/**
 * Function specialization for type 'bool'
 */
template<>
void parseKeyVal<bool,bool>(util::ConfigStore const& configStore,
                            string const& key,
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

string ConfigurationStore::_classMethodContext(string const& func) {
    return "ConfigurationStore::" + func;
}

ConfigurationStore::ConfigurationStore(util::ConfigStore const& configStore)
    :   Configuration(),
        _log(LOG_GET("lsst.qserv.replica.ConfigurationStore")) {

    _loadConfiguration(configStore);
}


void ConfigurationStore::addWorker(WorkerInfo const& info) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << info.name);
    util::Lock lock(_mtx, context(__func__));

    auto itr = _workerInfo.find(info.name);
    if (_workerInfo.end() != itr) {
        throw invalid_argument(
                _classMethodContext(__func__) + "  worker: " + info.name +
                " already exists");
    }
    
    // Scan existing workers to make sure no conflict on the same combination
    // of host:port exists
    
    for (auto const& itr: _workerInfo) {
        if (itr.first == info.name) {
            throw invalid_argument(
                    _classMethodContext(__func__) + "  worker: " + info.name +
                    " already exists");
        }
        if (itr.second.svcHost == info.svcHost and itr.second.svcPort == info.svcPort) {
            throw invalid_argument(
                    _classMethodContext(__func__) + "  worker: " + itr.first +
                    " with a conflicting combination of the service host/port " +
                    itr.second.svcHost + ":" + to_string(itr.second.svcPort) +
                    " already exists");
        }
        if (itr.second.fsHost == info.fsHost and itr.second.fsPort == info.fsPort) {
            throw invalid_argument(
                    _classMethodContext(__func__) + "  worker: " + itr.first +
                    " with a conflicting combination of the file service host/port " +
                    itr.second.fsHost + ":" + to_string(itr.second.fsPort) +
                    " already exists");
        }
    }
    _workerInfo[info.name] = info;
}


void ConfigurationStore::deleteWorker(string const& name) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    _workerInfo.erase(itr);
}


WorkerInfo ConfigurationStore::disableWorker(string const& name,
                                             bool disable) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name
         << " disable=" << (disable ? "true" : "false"));

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.isEnabled = not disable;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerReadOnly(string const& name,
                                                 bool readOnly) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name
         << " readOnly=" << (readOnly ? "true" : "false"));

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.isReadOnly = readOnly;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerSvcHost(string const& name,
                                                string const& host) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " host=" << host);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.svcHost = host;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerSvcPort(string const& name,
                                                uint16_t port) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " port=" << port);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.svcPort = port;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerFsHost(string const& name,
                                               string const& host) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " host=" << host);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.fsHost = host;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerFsPort(string const& name,
                                               uint16_t port) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " port=" << port);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.fsPort = port;

    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerDataDir(string const& name,
                                                string const& dataDir) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " dataDir=" << dataDir);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.dataDir = dataDir;

    return itr->second;

}


WorkerInfo ConfigurationStore::setWorkerDbHost(std::string const& name,
                                               std::string const& host) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " host=" << host);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.dbHost = host;
    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerDbPort(std::string const& name,
                                               uint16_t port) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " port=" << port);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.dbPort = port;
    return itr->second;
}


WorkerInfo ConfigurationStore::setWorkerDbUser(std::string const& name,
                                               std::string const& user)  {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name=" << name << " user=" << user);

    util::Lock lock(_mtx, context(__func__));
    auto itr = safeFindWorker(lock, name, _classMethodContext(__func__));
    itr->second.dbUser = user;

    return itr->second;
}


DatabaseFamilyInfo ConfigurationStore::addDatabaseFamily(DatabaseFamilyInfo const& info) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  familyInfo: " << info);

    util::Lock lock(_mtx, context(__func__));
    
    if (info.name.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the family name can't be empty");
    }
    if (info.replicationLevel == 0) {
        throw invalid_argument(_classMethodContext(__func__) + "  the replication level can't be 0");
    }
    if (info.numStripes == 0) {
        throw invalid_argument(_classMethodContext(__func__) + "  the number of stripes level can't be 0");
    }
    if (info.numSubStripes == 0) {
        throw invalid_argument(_classMethodContext(__func__) + "  the number of sub-stripes level can't be 0");
    }
    if (_databaseFamilyInfo.end() != _databaseFamilyInfo.find(info.name)) {
        throw invalid_argument(_classMethodContext(__func__) + "  the family already exists");
    }
    _databaseFamilyInfo[info.name] = DatabaseFamilyInfo{
        info.name,
        info.replicationLevel,
        info.numStripes,
        info.numSubStripes,
        make_shared<ChunkNumberQservValidator>(
            static_cast<int32_t>(info.numStripes),
            static_cast<int32_t>(info.numSubStripes))
    };
    return _databaseFamilyInfo[info.name];
}


void ConfigurationStore::deleteDatabaseFamily(string const& name) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name: " << name);

    util::Lock lock(_mtx, context(__func__));

    if (name.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the family name can't be empty");
    }
    
    // Find and delete the family
    auto itr = _databaseFamilyInfo.find(name);
    if (itr == _databaseFamilyInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  unknown family");
    }
    _databaseFamilyInfo.erase(itr);

    // Find and delete the relevant databases
    for(auto itr = _databaseInfo.begin(); itr != _databaseInfo.end();) {
        if (itr->second.family == name) {
            itr = _databaseInfo.erase(itr);     // the iterator now points past the erased element
        } else {
            ++itr;
        }
    }
}


DatabaseInfo ConfigurationStore::addDatabase(DatabaseInfo const& info) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  databaseInfo: " << info);

    util::Lock lock(_mtx, context(__func__));
    
    if (info.name.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the database name can't be empty");
    }
    if (info.family.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the family name can't be empty");
    }
    if (_databaseFamilyInfo.find(info.family) == _databaseFamilyInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  unknown database family: '" + info.family + "'");
    }
    if (_databaseInfo.find(info.name) != _databaseInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  database already exists");
    }
    _databaseInfo[info.name] = DatabaseInfo{
        info.name,
        info.family,
        {},
        {}
    };
    return _databaseInfo[info.name];
}


void ConfigurationStore::deleteDatabase(string const& name) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  name: " << name);

    util::Lock lock(_mtx, context(__func__));

    if (name.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the database name can't be empty");
    }
    
    // Find and delete the database
    auto itr = _databaseInfo.find(name);
    if (itr == _databaseInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  unknown database");
    }
    _databaseInfo.erase(itr);
}


DatabaseInfo ConfigurationStore::addTable(string const& database,
                                          string const& table,
                                          bool isPartitioned) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  database: " << database
         << " table: " << table << " isPartitioned: " << (isPartitioned ? "true" : "false"));

    util::Lock lock(_mtx, context(__func__));

    if (database.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the database name can't be empty");
    }
    if (table.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the table name can't be empty");
    }

    // Find the database
    auto itr = _databaseInfo.find(database);
    if (itr == _databaseInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  unknown database");
    }
    DatabaseInfo& info = itr->second;

    // Find the table
    if (find(info.partitionedTables.cbegin(),
             info.partitionedTables.cend(),
             table) != info.partitionedTables.cend() or
        find(info.regularTables.cbegin(), 
             info.regularTables.cend(),
             table) != info.regularTables.cend()) {

        throw invalid_argument(_classMethodContext(__func__) + "  table already exists");
    }

    // Insert the table into the corresponding collection
    if (isPartitioned) {
        info.partitionedTables.push_back(table);
    } else {
        info.regularTables.push_back(table);
    }
    return info;
}


DatabaseInfo ConfigurationStore::deleteTable(string const& database,
                                             string const& table) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  database: " << database
         << " table: " << table);

    util::Lock lock(_mtx, context(__func__));

    if (database.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the database name can't be empty");
    }
    if (table.empty()) {
        throw invalid_argument(_classMethodContext(__func__) + "  the table name can't be empty");
    }
    
    // Find the database
    auto itr = _databaseInfo.find(database);
    if (itr == _databaseInfo.end()) {
        throw invalid_argument(_classMethodContext(__func__) + "  unknown database");
    }
    DatabaseInfo& info = itr->second;

    auto pTableItr = find(info.partitionedTables.cbegin(),
                          info.partitionedTables.cend(),
                          table);
    if (pTableItr != info.partitionedTables.cend()) {
        info.partitionedTables.erase(pTableItr);
        return info;
    }
    auto rTableItr = find(info.regularTables.cbegin(),
                          info.regularTables.cend(),
                          table);
    if (rTableItr != info.regularTables.cend()) {
        info.regularTables.erase(rTableItr);
        return info;
    }
    throw invalid_argument(_classMethodContext(__func__) + "  unknown table");
}


void ConfigurationStore::_loadConfiguration(util::ConfigStore const& configStore) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    // Parse the list of worker names

    vector<string> workers;
    {
        istringstream ss(configStore.getRequired("common.workers"));
        istream_iterator<string> begin(ss), end;
        workers = vector<string>(begin, end);
    }
    vector<string> databaseFamilies;
    {
        istringstream ss(configStore.getRequired("common.database_families"));
        istream_iterator<string> begin(ss), end;
        databaseFamilies = vector<string>(begin, end);
    }
    vector<string> databases;
    {
        istringstream ss(configStore.getRequired("common.databases"));
        istream_iterator<string> begin(ss), end;
        databases = vector<string>(begin, end);
    }

    ::parseKeyVal(configStore, "common.request_buf_size_bytes",     _requestBufferSizeBytes,       defaultRequestBufferSizeBytes);
    ::parseKeyVal(configStore, "common.request_retry_interval_sec", _retryTimeoutSec,              defaultRetryTimeoutSec);

    ::parseKeyVal(configStore, "controller.num_threads",         _controllerThreads,           defaultControllerThreads);
    ::parseKeyVal(configStore, "controller.http_server_port",    _controllerHttpPort,          defaultControllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads", _controllerHttpThreads,       defaultControllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec", _controllerRequestTimeoutSec, defaultControllerRequestTimeoutSec);
    ::parseKeyVal(configStore, "controller.job_timeout_sec",     _jobTimeoutSec,               defaultJobTimeoutSec);
    ::parseKeyVal(configStore, "controller.job_heartbeat_sec",   _jobHeartbeatTimeoutSec,      defaultJobHeartbeatTimeoutSec);

    ::parseKeyVal(configStore, "database.technology",         _databaseTechnology,       defaultDatabaseTechnology);
    ::parseKeyVal(configStore, "database.host",               _databaseHost,             defaultDatabaseHost);
    ::parseKeyVal(configStore, "database.port",               _databasePort,             defaultDatabasePort);
    ::parseKeyVal(configStore, "database.user",               _databaseUser,             defaultDatabaseUser);
    ::parseKeyVal(configStore, "database.password",           _databasePassword,         defaultDatabasePassword);
    ::parseKeyVal(configStore, "database.name",               _databaseName,             defaultDatabaseName);
    ::parseKeyVal(configStore, "database.services_pool_size", _databaseServicesPoolSize, defaultDatabaseServicesPoolSize);

    ::parseKeyVal(configStore, "database.qserv_master_host",               _qservMasterDatabaseHost,             defaultQservMasterDatabaseHost);
    ::parseKeyVal(configStore, "database.qserv_master_port",               _qservMasterDatabasePort,             defaultQservMasterDatabasePort);
    ::parseKeyVal(configStore, "database.qserv_master_user",               _qservMasterDatabaseUser,             defaultQservMasterDatabaseUser);
    ::parseKeyVal(configStore, "database.qserv_master_password",           _qservMasterDatabasePassword,         defaultQservMasterDatabasePassword);
    ::parseKeyVal(configStore, "database.qserv_master_name",               _qservMasterDatabaseName,             defaultQservMasterDatabaseName);
    ::parseKeyVal(configStore, "database.qserv_master_services_pool_size", _qservMasterDatabaseServicesPoolSize, defaultQservMasterDatabaseServicesPoolSize);

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
    string   commonDataDir;
    uint16_t commonWorkerDbPort;
    string   commonWorkerDbUser;

    ::parseKeyVal(configStore, "worker.svc_port",    commonWorkerSvcPort,    defaultWorkerSvcPort);
    ::parseKeyVal(configStore, "worker.fs_port",     commonWorkerFsPort,     defaultWorkerFsPort);
    ::parseKeyVal(configStore, "worker.data_dir",    commonDataDir,          defaultDataDir);
    ::parseKeyVal(configStore, "worker.db_port",     commonWorkerDbPort,     defaultWorkerDbPort);
    ::parseKeyVal(configStore, "worker.db_user",     commonWorkerDbUser,     defaultWorkerDbUser);

    // Parse optional worker-specific configuration sections. Assume default
    // or (previously parsed) common values if a whole section or individual
    // parameters are missing.

    for (string const& name: workers) {

        string const section = "worker:" + name;
        if (_workerInfo.count(name)) {
            throw range_error(
                    _classMethodContext(__func__) + "  duplicate worker entry: '" +
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
        ::parseKeyVal(configStore, section+".db_host",      workerInfo.dbHost,     defaultWorkerDbHost);
        ::parseKeyVal(configStore, section+".db_port",      workerInfo.dbPort,     commonWorkerDbPort);
        ::parseKeyVal(configStore, section+".db_user",      workerInfo.dbUser,     commonWorkerDbUser);

        Configuration::translateDataDir(workerInfo.dataDir, name);
    }

    // Parse mandatory database family-specific configuration sections

    for (string const& name: databaseFamilies) {
        string const section = "database_family:" + name;
        if (_databaseFamilyInfo.count(name)) {
            throw range_error(
                    _classMethodContext(__func__) + "  duplicate database family entry: '" +
                    name + "' in: [common] or ["+section+"]");
        }
        _databaseFamilyInfo[name].name = name;
        ::parseKeyVal(configStore,
                      section+".min_replication_level",
                      _databaseFamilyInfo[name].replicationLevel,
                      defaultReplicationLevel);
        if (not _databaseFamilyInfo[name].replicationLevel) {
            _databaseFamilyInfo[name].replicationLevel= defaultReplicationLevel;
        }
        ::parseKeyVal(configStore,
                      section+".num_stripes",
                      _databaseFamilyInfo[name].numStripes,
                      defaultNumStripes);
        if (not _databaseFamilyInfo[name].numStripes) {
            _databaseFamilyInfo[name].numStripes= defaultNumStripes;
        }
        ::parseKeyVal(configStore,
                      section+".num_sub_stripes",
                      _databaseFamilyInfo[name].numSubStripes,
                      defaultNumSubStripes);
        if (not _databaseFamilyInfo[name].numSubStripes) {
            _databaseFamilyInfo[name].numSubStripes= defaultNumSubStripes;
        }
        _databaseFamilyInfo[name].chunkNumberValidator =
            make_shared<ChunkNumberQservValidator>(
                    static_cast<int32_t>(_databaseFamilyInfo[name].numStripes),
                    static_cast<int32_t>(_databaseFamilyInfo[name].numSubStripes));
    }

    // Parse mandatory database-specific configuration sections

    for (string const& name: databases) {

        string const section = "database:" + name;
        if (_databaseInfo.count(name)) {
            throw range_error(
                    _classMethodContext(__func__) + "  duplicate database entry: '" +
                    name + "' in: [common] or ["+section+"]");
        }
        _databaseInfo[name].name = name;
        _databaseInfo[name].family = configStore.getRequired(section+".family");
        if (not _databaseFamilyInfo.count(_databaseInfo[name].family)) {
            throw range_error(
                    _classMethodContext(__func__) + "  unknown database family: '" +
                    _databaseInfo[name].family + "' in section ["+section+"]");
        }
        {
            istringstream ss(configStore.getRequired(section+".partitioned_tables"));
            istream_iterator<string> begin(ss), end;
            _databaseInfo[name].partitionedTables = vector<string>(begin, end);
        }
        {
            istringstream ss(configStore.getRequired(section+".regular_tables"));
            istream_iterator<string> begin(ss), end;
            _databaseInfo[name].regularTables = vector<string>(begin, end);
        }
    }
    dumpIntoLogger();
}

}}} // namespace lsst::qserv::replica
