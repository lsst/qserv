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
#include "replica/Configuration.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ChunkNumber.h"
#include "replica/ConfigurationFile.h"
#include "replica/ConfigurationMap.h"
#include "replica/ConfigurationMySQL.h"
#include "replica/FileUtils.h"
#include "util/IterableFormatter.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

json WorkerInfo::toJson() const {

    json infoJson;

    infoJson["name"]         = name;
    infoJson["is_enabled"]   = isEnabled  ? 1 : 0;
    infoJson["is_read_only"] = isReadOnly ? 1 : 0;
    infoJson["svc_host"]     = svcHost;
    infoJson["svc_port"]     = svcPort;
    infoJson["fs_host"]      = fsHost;
    infoJson["fs_port"]      = fsPort;
    infoJson["data_dir"]     = dataDir;
    infoJson["db_host"]      = dbHost;
    infoJson["db_port"]      = dbPort;
    infoJson["db_user"]      = dbUser;

    return infoJson;
}


json DatabaseInfo::toJson() const {

    json infoJson;

    infoJson["name"] = name;

    for (auto&& name: partitionedTables) {
        infoJson["tables"].push_back({
            {"name",           name},
            {"is_partitioned", 1}});
    }
    for (auto&& name: regularTables) {
        infoJson["tables"].push_back({
            {"name",           name},
            {"is_partitioned", 0}});
    }
    return infoJson;
}


json DatabaseFamilyInfo::toJson() const {

    json infoJson;

    infoJson["name"]                  = name;
    infoJson["min_replication_level"] = replicationLevel;
    infoJson["num_stripes"]           = numStripes;
    infoJson["num_sub_stripes"]       = numSubStripes;

    return infoJson;
}


ostream& operator <<(ostream& os, WorkerInfo const& info) {
    os  << "WorkerInfo ("
        << "name:'"      <<      info.name       << "',"
        << "isEnabled:"  << (int)info.isEnabled  << ","
        << "isReadOnly:" << (int)info.isReadOnly << ","
        << "svcHost:'"   <<      info.svcHost    << "',"
        << "svcPort:"    <<      info.svcPort    << ","
        << "fsHost:'"    <<      info.fsHost     << "',"
        << "fsPort:"     <<      info.fsPort     << ","
        << "dataDir:'"   <<      info.dataDir    << "',"
        << "dbHost:'"    <<      info.dbHost     << "',"
        << "dbPort:"     <<      info.dbPort     << ","
        << "dbUser:'"    <<      info.dbUser     << "')";
    return os;
}


ostream& operator <<(ostream& os, DatabaseInfo const& info) {
    os  << "DatabaseInfo ("
        << "name:'" << info.name << "',"
        << "family:'" << info.family << "',"
        << "partitionedTables:" << util::printable(info.partitionedTables) << ","
        << "regularTables:" << util::printable(info.regularTables) << ")";
    return os;
}


ostream& operator <<(ostream& os, DatabaseFamilyInfo const& info) {
    os  << "DatabaseFamilyInfo ("
        << "name:'" << info.name << "',"
        << "replicationLevel:'" << info.replicationLevel << "',"
        << "numStripes:" << info.numStripes << ","
        << "numSubStripes:" << info.numSubStripes << ")";
    return os;
}


Configuration::Ptr Configuration::load(string const& configUrl) {

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
                    Configuration::defaultDatabaseHost,
                    Configuration::defaultDatabasePort,
                    Configuration::defaultDatabaseUser,
                    Configuration::defaultDatabasePassword
                )
            );
        }
    }
    throw invalid_argument(
            "Configuration::" + string(__func__) + "  configUrl must start with 'file:' or 'mysql:'");
}


Configuration::Ptr Configuration::load(map<string, string> const& kvMap) {
    return make_shared<ConfigurationMap>(kvMap);
}


// Set some reasonable defaults

size_t       const Configuration::defaultRequestBufferSizeBytes       = 1024;
unsigned int const Configuration::defaultRetryTimeoutSec              = 1;
size_t       const Configuration::defaultControllerThreads            = 1;
uint16_t     const Configuration::defaultControllerHttpPort           = 80;
size_t       const Configuration::defaultControllerHttpThreads        = 1;
unsigned int const Configuration::defaultControllerRequestTimeoutSec  = 3600;
unsigned int const Configuration::defaultJobTimeoutSec                = 6000;
unsigned int const Configuration::defaultJobHeartbeatTimeoutSec       = 60;
bool         const Configuration::defaultXrootdAutoNotify             = false;
string       const Configuration::defaultXrootdHost                   = "localhost";
uint16_t     const Configuration::defaultXrootdPort                   = 1094;
unsigned int const Configuration::defaultXrootdTimeoutSec             = 3600;
string       const Configuration::defaultWorkerTechnology             = "TEST";
size_t       const Configuration::defaultWorkerNumProcessingThreads   = 1;
size_t       const Configuration::defaultFsNumProcessingThreads       = 1;
size_t       const Configuration::defaultWorkerFsBufferSizeBytes      = 1048576;
string       const Configuration::defaultWorkerSvcHost                = "localhost";
uint16_t     const Configuration::defaultWorkerSvcPort                = 50000;
string       const Configuration::defaultWorkerFsHost                 = "localhost";
uint16_t     const Configuration::defaultWorkerFsPort                 = 50001;
string       const Configuration::defaultDataDir                      = "{worker}";
string       const Configuration::defaultWorkerDbHost                 = "localhost";
uint16_t     const Configuration::defaultWorkerDbPort                 = 3306;
string       const Configuration::defaultWorkerDbUser                 = FileUtils::getEffectiveUser();
string       const Configuration::defaultDatabaseTechnology           = "mysql";
string       const Configuration::defaultDatabaseHost                 = "localhost";
uint16_t     const Configuration::defaultDatabasePort                 = 3306;
string       const Configuration::defaultDatabaseUser                 = FileUtils::getEffectiveUser();
string       const Configuration::defaultDatabasePassword             = "";
string       const Configuration::defaultDatabaseName                 = "qservReplica";
size_t       const Configuration::defaultDatabaseServicesPoolSize     = 1;
string       const Configuration::defaultQservMasterDatabaseHost      = "localhost";
uint16_t     const Configuration::defaultQservMasterDatabasePort      = 3306;
string       const Configuration::defaultQservMasterDatabaseUser      = FileUtils::getEffectiveUser();
string       const Configuration::defaultQservMasterDatabasePassword  = "";
string       const Configuration::defaultQservMasterDatabaseName      = "qservMeta";
size_t       const Configuration::defaultQservMasterDatabaseServicesPoolSize = 1;
bool               Configuration::defaultDatabaseAllowReconnect       = true;
unsigned int       Configuration::defaultDatabaseConnectTimeoutSec    = 3600;
unsigned int       Configuration::defaultDatabaseMaxReconnects        = 1;
unsigned int       Configuration::defaultDatabaseTransactionTimeoutSec= 3600;
size_t       const Configuration::defaultReplicationLevel             = 1;
unsigned int const Configuration::defaultNumStripes                   = 340;
unsigned int const Configuration::defaultNumSubStripes                = 12;


void Configuration::translateDataDir(string& dataDir,
                                     string const& workerName) {

    string::size_type const leftPos = dataDir.find('{');
    if (leftPos == string::npos) return;

    string::size_type const rightPos = dataDir.find('}');
    if (rightPos == string::npos) return;

    if (rightPos <= leftPos) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  invalid template in the data directory path: '" +
                dataDir + "'");
    }
    if (dataDir.substr (leftPos, rightPos - leftPos + 1) == "{worker}") {
        dataDir.replace(leftPos, rightPos - leftPos + 1, workerName);
    }
}


Configuration::Configuration()
    :   _requestBufferSizeBytes     (defaultRequestBufferSizeBytes),
        _retryTimeoutSec            (defaultRetryTimeoutSec),
        _controllerThreads          (defaultControllerThreads),
        _controllerHttpPort         (defaultControllerHttpPort),
        _controllerHttpThreads      (defaultControllerHttpThreads),
        _controllerRequestTimeoutSec(defaultControllerRequestTimeoutSec),
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
        _qservMasterDatabasePassword(defaultQservMasterDatabasePassword),
        _qservMasterDatabaseName    (defaultQservMasterDatabaseName),
        _qservMasterDatabaseServicesPoolSize(defaultQservMasterDatabaseServicesPoolSize) {
}


string Configuration::context(string const& func) const {
    string const str = "CONFIG   " + func;
    return str;
}


vector<string> Configuration::workers(bool isEnabled,
                                      bool isReadOnly) const {

    util::Lock lock(_mtx, context(__func__));

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


vector<string> Configuration::allWorkers() const {
    util::Lock lock(_mtx, context(__func__));
    vector<string> names;
    for (auto&& entry: _workerInfo) {
        auto const& name = entry.first;
        names.push_back(name);
    }
    return names;
}


vector<string> Configuration::databaseFamilies() const {

    util::Lock lock(_mtx, context(__func__));

    vector<string> families;
    for (auto&& itr: _databaseFamilyInfo) {
        families.push_back(itr.first);
    }
    return families;
}


bool Configuration::isKnownDatabaseFamily(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    return _databaseFamilyInfo.count(name);
}


size_t Configuration::replicationLevel(string const& family) const {

    util::Lock lock(_mtx, context(__func__));

    auto const itr = _databaseFamilyInfo.find(family);
    if (itr == _databaseFamilyInfo.end()) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  unknown database family: '" +
                family + "'");
    }
    return itr->second.replicationLevel;
}


DatabaseFamilyInfo Configuration::databaseFamilyInfo(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    auto&& itr = _databaseFamilyInfo.find(name);
    if (itr == _databaseFamilyInfo.end()) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  unknown database family: '" + name + "'");
    }
    return itr->second;
}


vector<string> Configuration::databases(string const& family) const {

    util::Lock lock(_mtx, context() + string(__func__) + "(family)");

    if (not family.empty() and not _databaseFamilyInfo.count(family)) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  unknown database family: '" +
                family + "'");
    }
    vector<string> names;
    for (auto&& entry: _databaseInfo) {
        if (not family.empty() and (family != entry.second.family)) {
            continue;
        }
        names.push_back(entry.first);
    }
    return names;
}


bool Configuration::isKnownWorker(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    return _workerInfo.count(name) > 0;
}


WorkerInfo Configuration::workerInfo(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    auto const itr = _workerInfo.find(name);
    if (itr == _workerInfo.end()) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  unknown worker: '" + name + "'");
    }
    return itr->second;
}


bool Configuration::isKnownDatabase(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    return _databaseInfo.count(name) > 0;
}


DatabaseInfo Configuration::databaseInfo(string const& name) const {

    util::Lock lock(_mtx, context(__func__));

    auto&& itr = _databaseInfo.find(name);
    if (itr == _databaseInfo.end()) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  unknown database: '" + name + "'");
    }
    return itr->second;
}


bool Configuration::setDatabaseAllowReconnect(bool value) {
    swap(value, defaultDatabaseAllowReconnect);
    return value;
}


unsigned int Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, defaultDatabaseConnectTimeoutSec);
    return value;
}


unsigned int Configuration::setDatabaseMaxReconnects(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, defaultDatabaseMaxReconnects);
    return value;
}


unsigned int Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, defaultDatabaseTransactionTimeoutSec);
    return value;
}


void Configuration::dumpIntoLogger() const {
    LOGS(_log, LOG_LVL_DEBUG, asString());
}


string Configuration::asString() const {

    ostringstream ss;
    ss << context() << "defaultRequestBufferSizeBytes:        " << defaultRequestBufferSizeBytes << "\n";
    ss << context() << "defaultRetryTimeoutSec:               " << defaultRetryTimeoutSec << "\n";
    ss << context() << "defaultControllerThreads:             " << defaultControllerThreads << "\n";
    ss << context() << "defaultControllerHttpPort:            " << defaultControllerHttpPort << "\n";
    ss << context() << "defaultControllerHttpThreads:         " << defaultControllerHttpThreads << "\n";
    ss << context() << "defaultControllerRequestTimeoutSec:   " << defaultControllerRequestTimeoutSec << "\n";
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
    ss << context() << "defaultWorkerSvcHost:                 " << defaultWorkerSvcHost << "\n";
    ss << context() << "defaultWorkerSvcPort:                 " << defaultWorkerSvcPort << "\n";
    ss << context() << "defaultWorkerFsHost:                  " << defaultWorkerFsHost << "\n";
    ss << context() << "defaultWorkerFsPort:                  " << defaultWorkerFsPort << "\n";
    ss << context() << "defaultDataDir:                       " << defaultDataDir << "\n";
    ss << context() << "defaultWorkerDbHost:                  " << defaultWorkerDbHost << "\n";
    ss << context() << "defaultWorkerDbPort:                  " << defaultWorkerDbPort << "\n";
    ss << context() << "defaultWorkerDbUser:                  " << defaultWorkerDbUser << "\n";
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
    ss << context() << "defaultDatabaseAllowReconnect:        " << (defaultDatabaseAllowReconnect ? "1" : "0") << "\n";
    ss << context() << "defaultDatabaseConnectTimeoutSec:     " << defaultDatabaseConnectTimeoutSec << "\n";
    ss << context() << "defaultDatabaseMaxReconnects:         " << defaultDatabaseMaxReconnects << "\n";
    ss << context() << "defaultDatabaseTransactionTimeoutSec: " << defaultDatabaseTransactionTimeoutSec << "\n";
    ss << context() << "defaultReplicationLevel:              " << defaultReplicationLevel << "\n";
    ss << context() << "defaultNumStripes:                    " << defaultNumStripes << "\n";
    ss << context() << "defaultNumSubStripes:                 " << defaultNumSubStripes << "\n";
    ss << context() << "_requestBufferSizeBytes:              " << _requestBufferSizeBytes << "\n";
    ss << context() << "_retryTimeoutSec:                     " << _retryTimeoutSec << "\n";
    ss << context() << "_controllerThreads:                   " << _controllerThreads << "\n";
    ss << context() << "_controllerHttpPort:                  " << _controllerHttpPort << "\n";
    ss << context() << "_controllerHttpThreads:               " << _controllerHttpThreads << "\n";
    ss << context() << "_controllerRequestTimeoutSec:         " << _controllerRequestTimeoutSec << "\n";
    ss << context() << "_jobTimeoutSec:                       " << _jobTimeoutSec << "\n";
    ss << context() << "_jobHeartbeatTimeoutSec:              " << _jobHeartbeatTimeoutSec << "\n";
    ss << context() << "_xrootdAutoNotify:                    " << (_xrootdAutoNotify ? "1" : "0") << "\n";
    ss << context() << "_xrootdHost:                          " << _xrootdHost << "\n";
    ss << context() << "_xrootdPort:                          " << _xrootdPort << "\n";
    ss << context() << "_xrootdTimeoutSec:                    " << _xrootdTimeoutSec << "\n";
    ss << context() << "_workerTechnology:                    " << _workerTechnology << "\n";
    ss << context() << "_workerNumProcessingThreads:          " << _workerNumProcessingThreads << "\n";
    ss << context() << "_fsNumProcessingThreads:              " << _fsNumProcessingThreads << "\n";
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


map<string, WorkerInfo>::iterator Configuration::safeFindWorker(util::Lock const& lock,
                                                                string const& name,
                                                                string const& context) {
    auto itr = _workerInfo.find(name);
    if (_workerInfo.end() != itr) return itr;
    throw invalid_argument(context + "  no such worker: " + name);
}

}}} // namespace lsst::qserv::replica
