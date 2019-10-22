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
#include "replica/ConfigurationBase.h"

// System headers
#include <algorithm>
#include <set>
#include <stdexcept>

// Qserv headers
#include "global/constants.h"
#include "replica/ConfigurationFile.h"
#include "replica/ConfigurationMap.h"
#include "replica/ConfigurationMySQL.h"
#include "replica/FileUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationBase");

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

// Set some reasonable defaults

size_t       const ConfigurationBase::defaultRequestBufferSizeBytes       = 1024;
unsigned int const ConfigurationBase::defaultRetryTimeoutSec              = 1;
size_t       const ConfigurationBase::defaultControllerThreads            = 1;
uint16_t     const ConfigurationBase::defaultControllerHttpPort           = 80;
size_t       const ConfigurationBase::defaultControllerHttpThreads        = 1;
unsigned int const ConfigurationBase::defaultControllerRequestTimeoutSec  = 3600;
std::string  const ConfigurationBase::defaultControllerEmptyChunksDir     = "data/{worker}";
unsigned int const ConfigurationBase::defaultJobTimeoutSec                = 6000;
unsigned int const ConfigurationBase::defaultJobHeartbeatTimeoutSec       = 60;
bool         const ConfigurationBase::defaultXrootdAutoNotify             = false;
string       const ConfigurationBase::defaultXrootdHost                   = "localhost";
uint16_t     const ConfigurationBase::defaultXrootdPort                   = 1094;
unsigned int const ConfigurationBase::defaultXrootdTimeoutSec             = 3600;
string       const ConfigurationBase::defaultWorkerTechnology             = "TEST";
size_t       const ConfigurationBase::defaultWorkerNumProcessingThreads   = 1;
size_t       const ConfigurationBase::defaultFsNumProcessingThreads       = 1;
size_t       const ConfigurationBase::defaultWorkerFsBufferSizeBytes      = 1048576;
size_t       const ConfigurationBase::defaultLoaderNumProcessingThreads   = 1;
string       const ConfigurationBase::defaultWorkerSvcHost                = "localhost";
uint16_t     const ConfigurationBase::defaultWorkerSvcPort                = 50000;
string       const ConfigurationBase::defaultWorkerFsHost                 = "localhost";
uint16_t     const ConfigurationBase::defaultWorkerFsPort                 = 50001;
string       const ConfigurationBase::defaultDataDir                      = "data/{worker}";
string       const ConfigurationBase::defaultWorkerDbHost                 = "localhost";
uint16_t     const ConfigurationBase::defaultWorkerDbPort                 = 3306;
string       const ConfigurationBase::defaultWorkerDbUser                 = FileUtils::getEffectiveUser();
string       const ConfigurationBase::defaultWorkerLoaderHost             = "localhost";
uint16_t     const ConfigurationBase::defaultWorkerLoaderPort             = 50002;
string       const ConfigurationBase::defaultWorkerLoaderTmpDir           = "tmp/{worker}";
string       const ConfigurationBase::defaultDatabaseTechnology           = "mysql";
string       const ConfigurationBase::defaultDatabaseHost                 = "localhost";
uint16_t     const ConfigurationBase::defaultDatabasePort                 = 3306;
string       const ConfigurationBase::defaultDatabaseUser                 = FileUtils::getEffectiveUser();
string       const ConfigurationBase::defaultDatabasePassword             = "";
string       const ConfigurationBase::defaultDatabaseName                 = "qservReplica";
size_t       const ConfigurationBase::defaultDatabaseServicesPoolSize     = 1;
string       const ConfigurationBase::defaultQservMasterDatabaseHost      = "localhost";
uint16_t     const ConfigurationBase::defaultQservMasterDatabasePort      = 3306;
string       const ConfigurationBase::defaultQservMasterDatabaseUser      = FileUtils::getEffectiveUser();
string       const ConfigurationBase::defaultQservMasterDatabaseName      = lsst::qserv::SEC_INDEX_DB;
size_t       const ConfigurationBase::defaultQservMasterDatabaseServicesPoolSize = 1;
string       const ConfigurationBase::defaultQservMasterDatabaseTmpDir    = "/qserv/data/ingest";
size_t       const ConfigurationBase::defaultReplicationLevel             = 1;
unsigned int const ConfigurationBase::defaultNumStripes                   = 340;
unsigned int const ConfigurationBase::defaultNumSubStripes                = 12;




ConfigurationIFace::Ptr ConfigurationBase::load(string const& configUrl) {

    string::size_type const pos = configUrl.find(':');
    if (pos != string::npos) {

        string const prefix = configUrl.substr(0, pos);
        string const suffix = configUrl.substr(pos+1);

        if ("file"  == prefix) {
            return make_shared<ConfigurationFile>(suffix);
        } else if ("mysql" == prefix) {
            return make_shared<ConfigurationMySQL>(
                        database::mysql::ConnectionParams::parse(
                            configUrl,
                            ConfigurationBase::defaultDatabaseHost,
                            ConfigurationBase::defaultDatabasePort,
                            ConfigurationBase::defaultDatabaseUser,
                            ConfigurationBase::defaultDatabasePassword));
        }
    }
    throw invalid_argument(
            "ConfigurationBase::" + string(__func__) + "  configUrl must start with 'file:' or 'mysql:'");
}


ConfigurationIFace::Ptr ConfigurationBase::load(map<string, string> const& kvMap) {
    return make_shared<ConfigurationMap>(kvMap);
}


void ConfigurationBase::translateWorkerDir(string& path, string const& workerName) {

    string::size_type const leftPos = path.find('{');
    if (leftPos == string::npos) return;

    string::size_type const rightPos = path.find('}');
    if (rightPos == string::npos) return;

    if (rightPos <= leftPos) {
        throw invalid_argument(
                "ConfigurationBase::" + string(__func__) + "  invalid template in the worker directory path: '" +
                path + "'");
    }
    if (path.substr (leftPos, rightPos - leftPos + 1) == "{worker}") {
        path.replace(leftPos, rightPos - leftPos + 1, workerName);
    }
}


ConfigurationBase::ConfigurationBase()
    :   _requestBufferSizeBytes     (defaultRequestBufferSizeBytes),
        _retryTimeoutSec            (defaultRetryTimeoutSec),
        _controllerThreads          (defaultControllerThreads),
        _controllerHttpPort         (defaultControllerHttpPort),
        _controllerHttpThreads      (defaultControllerHttpThreads),
        _controllerRequestTimeoutSec(defaultControllerRequestTimeoutSec),
        _controllerEmptyChunksDir   (defaultControllerEmptyChunksDir),
        _jobTimeoutSec              (defaultJobTimeoutSec),
        _jobHeartbeatTimeoutSec     (defaultJobHeartbeatTimeoutSec),
        _xrootdAutoNotify           (defaultXrootdAutoNotify),
        _xrootdHost                 (defaultXrootdHost),
        _xrootdPort                 (defaultXrootdPort),
        _xrootdTimeoutSec           (defaultXrootdTimeoutSec),
        _workerTechnology           (defaultWorkerTechnology),
        _workerNumProcessingThreads (defaultWorkerNumProcessingThreads),
        _fsNumProcessingThreads     (defaultFsNumProcessingThreads),
        _workerFsBufferSizeBytes    (defaultWorkerFsBufferSizeBytes),
        _loaderNumProcessingThreads (defaultLoaderNumProcessingThreads),
        _databaseTechnology         (defaultDatabaseTechnology),
        _databaseHost               (defaultDatabaseHost),
        _databasePort               (defaultDatabasePort),
        _databaseUser               (defaultDatabaseUser),
        _databasePassword           (defaultDatabasePassword),
        _databaseName               (defaultDatabaseName),
        _databaseServicesPoolSize   (defaultDatabaseServicesPoolSize),
        _qservMasterDatabaseHost    (defaultQservMasterDatabaseHost),
        _qservMasterDatabasePort    (defaultQservMasterDatabasePort),
        _qservMasterDatabaseUser    (defaultQservMasterDatabaseUser),
        _qservMasterDatabaseName    (defaultQservMasterDatabaseName),
        _qservMasterDatabaseServicesPoolSize(defaultQservMasterDatabaseServicesPoolSize),
        _qservMasterDatabaseTmpDir  (defaultQservMasterDatabaseTmpDir) {
}


vector<string> ConfigurationBase::workers(bool isEnabled, bool isReadOnly) const {
    vector<string> names;
    for (auto&& entry: _workerInfo) {
        auto const& name = entry.first;
        auto const& info = entry.second;
        if (isEnabled) {
            if (info.isEnabled and (isReadOnly == info.isReadOnly)) {
                names.push_back(name);
            }
        } else {
            if (not info.isEnabled) {
                names.push_back(name);
            }
        }
    }
    return names;
}


vector<string> ConfigurationBase::allWorkers() const {
    vector<string> names;
    for (auto&& entry: _workerInfo) {
        auto const& name = entry.first;
        names.push_back(name);
    }
    return names;
}


vector<string> ConfigurationBase::databaseFamilies() const {
    vector<string> families;
    for (auto&& itr: _databaseFamilyInfo) {
        families.push_back(itr.first);
    }
    return families;
}


bool ConfigurationBase::isKnownDatabaseFamily(string const& name) const {
    return _databaseFamilyInfo.count(name);
}


size_t ConfigurationBase::replicationLevel(string const& family) const {
    auto const itr = _databaseFamilyInfo.find(family);
    if (itr == _databaseFamilyInfo.end()) {
        throw invalid_argument(
                "ConfigurationBase::" + string(__func__) + "  unknown database family: '" +
                family + "'");
    }
    return itr->second.replicationLevel;
}


DatabaseFamilyInfo ConfigurationBase::databaseFamilyInfo(string const& name) const {
    auto&& itr = _databaseFamilyInfo.find(name);
    if (itr == _databaseFamilyInfo.end()) {
        throw invalid_argument(
                "ConfigurationBase::" + string(__func__) + "  unknown database family: '" + name + "'");
    }
    return itr->second;
}


vector<string> ConfigurationBase::databases(string const& family, bool allDatabases, bool isPublished) const {

    string const context_ = context() + string(__func__) + " "
        + " family='" + family + "' allDatabases=" + string(allDatabases ? "1" : "0")
        + " isPublished=" + string(isPublished ? "1" : "0") + "  ";

    if (not family.empty() and not _databaseFamilyInfo.count(family)) {
        throw invalid_argument(context_ + "unknown database family");
    }
    vector<string> names;
    for (auto&& entry: _databaseInfo) {
        if (not family.empty() and (family != entry.second.family)) {
            continue;
        }
        if (not allDatabases) {
            // Logical XOR
            if ((    isPublished and not entry.second.isPublished) or
                (not isPublished and     entry.second.isPublished)) continue;
        }
        names.push_back(entry.first);
    }
    return names;
}


bool ConfigurationBase::isKnownWorker(string const& name) const {
    return _workerInfo.count(name) > 0;
}


WorkerInfo ConfigurationBase::workerInfo(string const& name) const {
    auto const itr = _workerInfo.find(name);
    if (itr == _workerInfo.end()) {
        throw invalid_argument(
                "ConfigurationBase::" + string(__func__) + "  unknown worker: '" + name + "'");
    }
    return itr->second;
}


bool ConfigurationBase::isKnownDatabase(string const& name) const {
    return _databaseInfo.count(name) > 0;
}


DatabaseInfo ConfigurationBase::databaseInfo(string const& name) const {
    auto&& itr = _databaseInfo.find(name);
    if (itr == _databaseInfo.end()) {
        throw invalid_argument(
                "ConfigurationBase::" + string(__func__) + "  unknown database: '" + name + "'");
    }
    return itr->second;
}


void ConfigurationBase::dumpIntoLogger() const {
    LOGS(_log, LOG_LVL_DEBUG, asString());
}


string ConfigurationBase::asString() const {

    ostringstream ss;
    ss << context() << "defaultRequestBufferSizeBytes:        " << defaultRequestBufferSizeBytes << "\n";
    ss << context() << "defaultRetryTimeoutSec:               " << defaultRetryTimeoutSec << "\n";
    ss << context() << "defaultControllerThreads:             " << defaultControllerThreads << "\n";
    ss << context() << "defaultControllerHttpPort:            " << defaultControllerHttpPort << "\n";
    ss << context() << "defaultControllerHttpThreads:         " << defaultControllerHttpThreads << "\n";
    ss << context() << "defaultControllerRequestTimeoutSec:   " << defaultControllerRequestTimeoutSec << "\n";
    ss << context() << "defaultControllerEmptyChunksDir:      " << defaultControllerEmptyChunksDir << "\n";
    ss << context() << "defaultJobTimeoutSec:                 " << defaultJobTimeoutSec << "\n";
    ss << context() << "defaultJobHeartbeatTimeoutSec:        " << defaultJobHeartbeatTimeoutSec << "\n";
    ss << context() << "defaultXrootdAutoNotify:              " << (defaultXrootdAutoNotify ? "1" : "0") << "\n";
    ss << context() << "defaultXrootdHost:                    " << defaultXrootdHost << "\n";
    ss << context() << "defaultXrootdPort:                    " << defaultXrootdPort << "\n";
    ss << context() << "defaultXrootdTimeoutSec:              " << defaultXrootdTimeoutSec << "\n";
    ss << context() << "defaultWorkerTechnology:              " << defaultWorkerTechnology << "\n";
    ss << context() << "defaultWorkerNumProcessingThreads:    " << defaultWorkerNumProcessingThreads << "\n";
    ss << context() << "defaultFsNumProcessingThreads:        " << defaultFsNumProcessingThreads << "\n";
    ss << context() << "defaultWorkerFsBufferSizeBytes:       " << defaultWorkerFsBufferSizeBytes << "\n";
    ss << context() << "defaultLoaderNumProcessingThreads:    " << defaultLoaderNumProcessingThreads << "\n";
    ss << context() << "defaultWorkerSvcHost:                 " << defaultWorkerSvcHost << "\n";
    ss << context() << "defaultWorkerSvcPort:                 " << defaultWorkerSvcPort << "\n";
    ss << context() << "defaultWorkerFsHost:                  " << defaultWorkerFsHost << "\n";
    ss << context() << "defaultWorkerFsPort:                  " << defaultWorkerFsPort << "\n";
    ss << context() << "defaultDataDir:                       " << defaultDataDir << "\n";
    ss << context() << "defaultWorkerDbHost:                  " << defaultWorkerDbHost << "\n";
    ss << context() << "defaultWorkerDbPort:                  " << defaultWorkerDbPort << "\n";
    ss << context() << "defaultWorkerDbUser:                  " << defaultWorkerDbUser << "\n";
    ss << context() << "defaultWorkerLoaderHost:              " << defaultWorkerLoaderHost << "\n";
    ss << context() << "defaultWorkerLoaderPort:              " << defaultWorkerLoaderPort << "\n";
    ss << context() << "defaultWorkerLoaderTmpDir:            " << defaultWorkerLoaderTmpDir << "\n";
    ss << context() << "defaultDatabaseTechnology:            " << defaultDatabaseTechnology << "\n";
    ss << context() << "defaultDatabaseHost:                  " << defaultDatabaseHost << "\n";
    ss << context() << "defaultDatabasePort:                  " << defaultDatabasePort << "\n";
    ss << context() << "defaultDatabaseUser:                  " << defaultDatabaseUser << "\n";
    ss << context() << "defaultDatabaseName:                  " << defaultDatabaseName << "\n";
    ss << context() << "defaultDatabaseServicesPoolSize:      " << defaultDatabaseServicesPoolSize << "\n";
    ss << context() << "defaultQservMasterDatabaseHost:             " << defaultQservMasterDatabaseHost << "\n";
    ss << context() << "defaultQservMasterDatabasePort:             " << defaultQservMasterDatabasePort << "\n";
    ss << context() << "defaultQservMasterDatabaseUser:             " << defaultQservMasterDatabaseUser << "\n";
    ss << context() << "defaultQservMasterDatabaseName:             " << defaultQservMasterDatabaseName << "\n";
    ss << context() << "defaultQservMasterDatabaseServicesPoolSize: " << defaultQservMasterDatabaseServicesPoolSize << "\n";
    ss << context() << "defaultQservMasterDatabaseTmpDir:           " << defaultQservMasterDatabaseTmpDir << "\n";
    ss << context() << "defaultReplicationLevel:              " << defaultReplicationLevel << "\n";
    ss << context() << "defaultNumStripes:                    " << defaultNumStripes << "\n";
    ss << context() << "defaultNumSubStripes:                 " << defaultNumSubStripes << "\n";
    ss << context() << "_requestBufferSizeBytes:              " << _requestBufferSizeBytes << "\n";
    ss << context() << "_retryTimeoutSec:                     " << _retryTimeoutSec << "\n";
    ss << context() << "_controllerThreads:                   " << _controllerThreads << "\n";
    ss << context() << "_controllerHttpPort:                  " << _controllerHttpPort << "\n";
    ss << context() << "_controllerHttpThreads:               " << _controllerHttpThreads << "\n";
    ss << context() << "_controllerRequestTimeoutSec:         " << _controllerRequestTimeoutSec << "\n";
    ss << context() << "_controllerEmptyChunksDir:            " << _controllerEmptyChunksDir << "\n";
    ss << context() << "_jobTimeoutSec:                       " << _jobTimeoutSec << "\n";
    ss << context() << "_jobHeartbeatTimeoutSec:              " << _jobHeartbeatTimeoutSec << "\n";
    ss << context() << "_xrootdAutoNotify:                    " << (_xrootdAutoNotify ? "1" : "0") << "\n";
    ss << context() << "_xrootdHost:                          " << _xrootdHost << "\n";
    ss << context() << "_xrootdPort:                          " << _xrootdPort << "\n";
    ss << context() << "_xrootdTimeoutSec:                    " << _xrootdTimeoutSec << "\n";
    ss << context() << "_workerTechnology:                    " << _workerTechnology << "\n";
    ss << context() << "_workerNumProcessingThreads:          " << _workerNumProcessingThreads << "\n";
    ss << context() << "_fsNumProcessingThreads:              " << _fsNumProcessingThreads << "\n";
    ss << context() << "_loaderNumProcessingThreads:          " << _loaderNumProcessingThreads << "\n";
    ss << context() << "_workerFsBufferSizeBytes:             " << _workerFsBufferSizeBytes << "\n";
    ss << context() << "_databaseTechnology:                  " << _databaseTechnology << "\n";
    ss << context() << "_databaseHost:                        " << _databaseHost << "\n";
    ss << context() << "_databasePort:                        " << _databasePort << "\n";
    ss << context() << "_databaseUser:                        " << _databaseUser << "\n";
    ss << context() << "_databaseName:                        " << _databaseName << "\n";
    ss << context() << "_databaseServicesPoolSize:            " << _databaseServicesPoolSize << "\n";
    ss << context() << "_qservMasterDatabaseHost:             " << _qservMasterDatabaseHost << "\n";
    ss << context() << "_qservMasterDatabasePort:             " << _qservMasterDatabasePort << "\n";
    ss << context() << "_qservMasterDatabaseUser:             " << _qservMasterDatabaseUser << "\n";
    ss << context() << "_qservMasterDatabaseName:             " << _qservMasterDatabaseName << "\n";
    ss << context() << "_qservMasterDatabaseServicesPoolSize: " << _qservMasterDatabaseServicesPoolSize << "\n";
    ss << context() << "_qservMasterDatabaseTmpDir:           " << _qservMasterDatabaseTmpDir << "\n";

    for (auto&& elem: _workerInfo) {
        ss << context() << elem.second << "\n";
    }
    for (auto&& elem: _databaseInfo) {
        ss << context() << elem.second << "\n";
    }
    for (auto&& elem: _databaseFamilyInfo) {
        ss << context()
             << "databaseFamilyInfo["<< elem.first << "]: " << elem.second << "\n";
    }
    return ss.str();
}


map<string, WorkerInfo>::iterator ConfigurationBase::safeFindWorker(
        string const& name,
        string const& context) {

    auto itr = _workerInfo.find(name);
    if (_workerInfo.end() != itr) return itr;
    throw invalid_argument(context + "  no such worker: " + name);
}


map<string, DatabaseInfo>::iterator ConfigurationBase::safeFindDatabase(
        string const& name,
        string const& context) {

    auto itr = _databaseInfo.find(name);
    if (_databaseInfo.end() != itr) return itr;
    throw invalid_argument(context + "  no such database: " + name);
}


bool ConfigurationBase::columnInSchema(string const& colName,
                                       list<pair<string,string>> const& columns) const {
    return columns.end() != find_if(
         columns.begin(),
         columns.end(),
         [&] (pair<string,string> const& coldef) {
             return coldef.first == colName;
         }
     );
}


void ConfigurationBase::validateTableParameters(
        string const& context_,
        string const& database,
        string const& table,
        bool isPartitioned,
        list<pair<string,string>> const& columns,
        bool isDirectorTable,
        string const& directorTableKey,
        string const& chunkIdColName,
        string const& subChunkIdColName,
        string const& latitudeColName,
        string const& longitudeColName) const {

    if (database.empty()) throw invalid_argument(context_ + "  the database name can't be empty");
    if (table.empty()) throw invalid_argument(context_ + "  the table name can't be empty");

    // Find the database (an exception will be thrown if not found)
    auto info = databaseInfo(database);

    // Find the table
    if (find(info.partitionedTables.cbegin(),
             info.partitionedTables.cend(),
             table) != info.partitionedTables.cend() or
        find(info.regularTables.cbegin(), 
             info.regularTables.cend(),
             table) != info.regularTables.cend()) {
        throw invalid_argument(context_ + "  table already exists");
    }

    // Validate flags and column names
    if (isPartitioned) {
        if (isDirectorTable) {
            if (not info.directorTable.empty()) {
                throw invalid_argument(
                        context_ + "  another table '" + info.directorTable +
                        "' was already claimed as the 'director' table.");
            }
            if (directorTableKey.empty()) {
                throw invalid_argument(
                        context_ + "  a valid column name must be provided"
                        " for the 'director' table");
            }
            if (not columnInSchema(directorTableKey, columns)) {
                throw invalid_argument(
                        context_ + "  a value of parameter 'directorTableKey'"
                        " provided for the 'director' table '" + table + "' doesn't match any column"
                        " in the table schema");                
            }
            if (not latitudeColName.empty()) {
                if (not columnInSchema(latitudeColName, columns)) {
                    throw invalid_argument(
                            context_ + "  a value '" + latitudeColName + "' of parameter 'latitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");                
                }
            }
            if (not longitudeColName.empty()) {
                if (not columnInSchema(longitudeColName, columns)) {
                    throw invalid_argument(
                            context_ + "  a value '" + longitudeColName + "' of parameter 'longitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");                
                }
            }
        }
        map<string,string> const colDefs = {
            {"chunkIdColName",    chunkIdColName},
            {"subChunkIdColName", subChunkIdColName}
        };
        for (auto&& entry: colDefs) {
            string const& role = entry.first;
            string const& colName = entry.second;
            if (colName.empty()) {
                throw invalid_argument(
                        context_ + "  a valid column name must be provided"
                        " for the '" + role + "' parameter of the partitioned table");
            }
            if (not columnInSchema(colName, columns)) {
                throw invalid_argument(
                        context_ + "  no matching column found in the provided"
                        " schema for name '" + colName + " as required by parameter '" + role +
                        "' of the partitioned table: '" + table + "'");                
            }
        }
    } else {
        if (isDirectorTable) {
            throw invalid_argument(context_ + "  regular tables can't be the 'director' ones");
        }
    }
}


DatabaseInfo ConfigurationBase::addTableTransient(
        string const& context_,
        string const& database,
        string const& table,
        bool isPartitioned,
        list<pair<string,string>> const& columns,
        bool isDirectorTable,
        string const& directorTableKey,
        string const& chunkIdColName,
        string const& subChunkIdColName,
        string const& latitudeColName,
        string const& longitudeColName) {

    auto& info = _databaseInfo[database];
    if (isPartitioned) {
        info.partitionedTables.push_back(table);
        if (isDirectorTable) {
            info.directorTable = table;
            info.directorTableKey = directorTableKey;
        }
        info.chunkIdColName = chunkIdColName;
        info.subChunkIdColName = subChunkIdColName;
        info.latitudeColName[table] = latitudeColName;
        info.longitudeColName[table] = longitudeColName;
    } else {
        info.regularTables.push_back(table);
    }
    info.columns[table] = columns;
    return info;
}
}}} // namespace lsst::qserv::replica
