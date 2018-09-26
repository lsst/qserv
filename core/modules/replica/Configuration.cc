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

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

std::ostream& operator <<(std::ostream& os, WorkerInfo const& info) {
    os  << "WorkerInfo ("
        << "name:'"      <<      info.name       << "',"
        << "isEnabled:"  << (int)info.isEnabled  << ","
        << "isReadOnly:" << (int)info.isReadOnly << ","
        << "svcHost:'"   <<      info.svcHost    << "',"
        << "svcPort:"    <<      info.svcPort    << ","
        << "fsHost:'"    <<      info.fsHost     << "',"
        << "fsPort:"     <<      info.fsPort     << ","
        << "dataDir:'"   <<      info.dataDir    << "')";
    return os;
}

std::ostream& operator <<(std::ostream& os, DatabaseInfo const& info) {
    os  << "DatabaseInfo ("
        << "name:'" << info.name << "',"
        << "family:'" << info.family << "',"
        << "partitionedTables:" << util::printable(info.partitionedTables) << ","
        << "regularTables:" << util::printable(info.regularTables) << ")";
    return os;
}

std::ostream& operator <<(std::ostream& os, DatabaseFamilyInfo const& info) {
    os  << "DatabaseFamilyInfo ("
        << "name:'" << info.name << "',"
        << "replicationLevel:'" << info.replicationLevel << "',"
        << "numStripes:" << info.numStripes << ","
        << "numSubStripes:" << info.numSubStripes << ")";
    return os;
}

Configuration::Ptr Configuration::load(std::string const& configUrl) {

    std::string::size_type const pos = configUrl.find(':');
    if (pos != std::string::npos) {

        std::string const prefix = configUrl.substr(0, pos);
        std::string const suffix = configUrl.substr(pos+1);

        if ("file"  == prefix) {
            return std::make_shared<ConfigurationFile>(suffix);

        } else if ("mysql" == prefix) {
            return std::make_shared<ConfigurationMySQL>(
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
    throw std::invalid_argument(
            "Configuration::load:  configUrl must start with 'file:' or 'mysql:'");
}

Configuration::Ptr Configuration::load(std::map<std::string, std::string> const& kvMap) {
    return std::make_shared<ConfigurationMap>(kvMap);
}

// Set some reasonable defaults

size_t       const Configuration::defaultRequestBufferSizeBytes       (1024);
unsigned int const Configuration::defaultRetryTimeoutSec              (1);
size_t       const Configuration::defaultControllerThreads            (1);
uint16_t     const Configuration::defaultControllerHttpPort           (80);
size_t       const Configuration::defaultControllerHttpThreads        (1);
unsigned int const Configuration::defaultControllerRequestTimeoutSec  (3600);
unsigned int const Configuration::defaultJobTimeoutSec                (6000);
unsigned int const Configuration::defaultJobHeartbeatTimeoutSec       (60);
bool         const Configuration::defaultXrootdAutoNotify             (false);
std::string  const Configuration::defaultXrootdHost                   ("localhost");
uint16_t     const Configuration::defaultXrootdPort                   (1094);
unsigned int const Configuration::defaultXrootdTimeoutSec             (3600);
std::string  const Configuration::defaultWorkerTechnology             ("TEST");
size_t       const Configuration::defaultWorkerNumProcessingThreads   (1);
size_t       const Configuration::defaultFsNumProcessingThreads       (1);
size_t       const Configuration::defaultWorkerFsBufferSizeBytes      (1048576);
std::string  const Configuration::defaultWorkerSvcHost                ("localhost");
uint16_t     const Configuration::defaultWorkerSvcPort                (50000);
std::string  const Configuration::defaultWorkerFsHost                 ("localhost");
uint16_t     const Configuration::defaultWorkerFsPort                 (50001);
std::string  const Configuration::defaultDataDir                      ("{worker}");
std::string  const Configuration::defaultDatabaseTechnology           ("mysql");
std::string  const Configuration::defaultDatabaseHost                 ("localhost");
uint16_t     const Configuration::defaultDatabasePort                 (3306);
std::string  const Configuration::defaultDatabaseUser                 (FileUtils::getEffectiveUser());
std::string  const Configuration::defaultDatabasePassword             ("");
std::string  const Configuration::defaultDatabaseName                 ("qservReplica");
size_t       const Configuration::defaultDatabaseServicesPoolSize     (1);
bool               Configuration::defaultDatabaseAllowReconnect       (true);
unsigned int       Configuration::defaultDatabaseConnectTimeoutSec    (3600);
unsigned int       Configuration::defaultDatabaseMaxReconnects        (1);
unsigned int       Configuration::defaultDatabaseTransactionTimeoutSec(3600);
size_t       const Configuration::defaultReplicationLevel             (1);
unsigned int const Configuration::defaultNumStripes                   (340);
unsigned int const Configuration::defaultNumSubStripes                (12);

void Configuration::translateDataDir(std::string&       dataDir,
                                     std::string const& workerName) {

    std::string::size_type const leftPos = dataDir.find('{');
    if (leftPos == std::string::npos) return;

    std::string::size_type const rightPos = dataDir.find('}');
    if (rightPos == std::string::npos) return;

    if (rightPos <= leftPos) {
        throw std::invalid_argument(
                "Configuration::translateDataDir  misformed template in the data directory path: '" +
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
        _databaseServicesPoolSize   (defaultDatabaseServicesPoolSize) {
}

std::string Configuration::context() const {
    std::string const str = "CONFIG   ";
    return str;
}

std::vector<std::string> Configuration::workers(bool isEnabled,
                                                bool isReadOnly) const {

    util::Lock lock(_mtx, context() + "workers");

    std::vector<std::string> names;
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

std::vector<std::string> Configuration::databaseFamilies() const {

    util::Lock lock(_mtx, context() + "databaseFamilies");

    std::set<std::string> familyNames;
    for (auto&& elem: _databaseInfo) {
        familyNames.insert(elem.second.family);
    }
    std::vector<std::string> families;
    for (auto&& name: familyNames) {
        families.push_back(name);
    }
    return families;
}

bool Configuration::isKnownDatabaseFamily(std::string const& name) const {

    util::Lock lock(_mtx, context() + "isKnownDatabaseFamily");

    return _databaseFamilyInfo.count(name);
}

size_t Configuration::replicationLevel(std::string const& family) const {

    util::Lock lock(_mtx, context() + "databaseFamilies");

    auto const itr = _databaseFamilyInfo.find(family);
    if (itr == _databaseFamilyInfo.end()) {
        throw std::invalid_argument(
                "Configuration::replicationLevel  unknown database family: '" +
                family + "'");
    }
    return itr->second.replicationLevel;
}

DatabaseFamilyInfo const Configuration::databaseFamilyInfo(std::string const& name) const {

    util::Lock lock(_mtx, context() + "databaseFamilyInfo");

    auto&& itr = _databaseFamilyInfo.find(name);
    if (itr == _databaseFamilyInfo.end()) {
        throw std::invalid_argument(
                "Configuration::databaseFamilyInfo  uknown database family: '" + name + "'");
    }
    return itr->second;
}

std::vector<std::string> Configuration::databases(std::string const& family) const {

    util::Lock lock(_mtx, context() + "databases(family)");

    if (not family.empty() and not _databaseFamilyInfo.count(family)) {
        throw std::invalid_argument(
                "Configuration::databases  unknown database family: '" +
                family + "'");
    }
    std::vector<std::string> names;
    for (auto&& entry: _databaseInfo) {
        if (not family.empty() and (family != entry.second.family)) {
            continue;
        }
        names.push_back(entry.first);
    }
    return names;
}

bool Configuration::isKnownWorker(std::string const& name) const {

    util::Lock lock(_mtx, context() + "isKnownWorker");

    return _workerInfo.count(name) > 0;
}

WorkerInfo const Configuration::workerInfo(std::string const& name) const {

    util::Lock lock(_mtx, context() + "workerInfo");

    auto const itr = _workerInfo.find(name);
    if (itr == _workerInfo.end()) {
        throw std::invalid_argument(
                "Configuration::workerInfo() uknown worker: '" + name + "'");
    }
    return itr->second;
}

bool Configuration::isKnownDatabase(std::string const& name) const {

    util::Lock lock(_mtx, context() + "isKnownDatabase");

    return _databaseInfo.count(name) > 0;
}

DatabaseInfo const Configuration::databaseInfo(std::string const& name) const {

    util::Lock lock(_mtx, context() + "databaseInfo");

    auto&& itr = _databaseInfo.find(name);
    if (itr == _databaseInfo.end()) {
        throw std::invalid_argument(
                "Configuration::databaseInfo() uknown database: '" + name + "'");
    }
    return itr->second;
}

bool Configuration::setDatabaseAllowReconnect(bool value) {
    std::swap(value, defaultDatabaseAllowReconnect);
    return value;
}

unsigned int Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw std::invalid_argument(
                "Configuration::setDatabaseConnectTimeoutSec:  0 is not allowed");
    }
    std::swap(value, defaultDatabaseConnectTimeoutSec);
    return value;
}

unsigned int Configuration::setDatabaseMaxReconnects(unsigned int value) {
    if (0 == value) {
        throw std::invalid_argument(
                "Configuration::setDatabaseMaxReconnects:  0 is not allowed");
    }
    std::swap(value, defaultDatabaseMaxReconnects);
    return value;
}

unsigned int Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw std::invalid_argument(
                "Configuration::setDatabaseTransactionTimeoutSec:  0 is not allowed");
    }
    std::swap(value, defaultDatabaseTransactionTimeoutSec);
    return value;
}

void Configuration::dumpIntoLogger() const {
    LOGS(_log, LOG_LVL_DEBUG, asString());
}

std::string Configuration::asString() const {

    std::ostringstream ss;
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
    ss << context() << "defaultDatabaseTechnology:            " << defaultDatabaseTechnology << "\n";
    ss << context() << "defaultDatabaseHost:                  " << defaultDatabaseHost << "\n";
    ss << context() << "defaultDatabasePort:                  " << defaultDatabasePort << "\n";
    ss << context() << "defaultDatabaseUser:                  " << defaultDatabaseUser << "\n";
    ss << context() << "defaultDatabasePassword:              " << "*****" << "\n";
    ss << context() << "defaultDatabaseName:                  " << defaultDatabaseName << "\n";
    ss << context() << "defaultDatabaseServicesPoolSize:      " << defaultDatabaseServicesPoolSize << "\n";
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
    ss << context() << "_databasePassword:                    " << "*****" << "\n";
    ss << context() << "_databaseName:                        " << _databaseName << "\n";
    ss << context() << "_databaseServicesPoolSize:            " << _databaseServicesPoolSize << "\n";
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

}}} // namespace lsst::qserv::replica
