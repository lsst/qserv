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
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ConfigurationFile.h"
#include "replica/ConfigurationMySQL.h"
#include "replica/FileUtils.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

/**
 * Print the content of a vector into the provided stream.
 * Assume that proper astreaming operator is defined for the vector
 * elements.
 *
 * @param os - the output stream object
 * @param v  - the vector to stream
 * @return   - the stream object
 */
template <typename T>
void vector2stream(std::ostream&         os,
                   std::vector<T> const& v) {
    for (size_t i = 0, num = v.size(); i < num; ++i) {
        os  << (i ? "," : "") << v[i];
    }
}

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
        << "partitionedTables:[";
    ::vector2stream(os, info.partitionedTables);
    os  << "],"
        << "regularTables:[";
    ::vector2stream(os, info.regularTables);
    os  << "])";
    return os;
}

Configuration::pointer Configuration::load(std::string const& configUrl) {

    for (auto const& proposedPrefix: std::vector<std::string>{"file:","mysql:"}) {

        std::string::size_type const prefixSize = proposedPrefix.size();
        std::string const            prefix = configUrl.substr(0, prefixSize);
        std::string const            suffix = configUrl.substr(prefixSize);

        if ("file:"  == prefix) {
            return Configuration::pointer(
                new ConfigurationFile(suffix));

        } else if ("mysql:" == prefix) {
            return Configuration::pointer(
                new ConfigurationMySQL(
                    database::mysql::ConnectionParams::parse(
                        suffix,
                        Configuration::defaultDatabaseHost,
                        Configuration::defaultDatabasePort,
                        Configuration::defaultDatabaseUser,
                        Configuration::defaultDatabasePassword)));
        }
    }
    throw std::invalid_argument(
            "Configuration::load:  unsupported configUrl: " + configUrl);
}


// Set some reasonable defaults

size_t       const Configuration::defaultRequestBufferSizeBytes      {1024};
unsigned int const Configuration::defaultRetryTimeoutSec             {1};
uint16_t     const Configuration::defaultControllerHttpPort          {80};
size_t       const Configuration::defaultControllerHttpThreads       {1};
unsigned int const Configuration::defaultControllerRequestTimeoutSec {3600};
unsigned int const Configuration::defaultJobTimeoutSec               {6000};
unsigned int const Configuration::defaultJobHeartbeatTimeoutSec      {60};
bool         const Configuration::defaultXrootdAutoNotify            {false};
std::string  const Configuration::defaultXrootdHost                  {"localhost"};
uint16_t     const Configuration::defaultXrootdPort                  {1094};
unsigned int const Configuration::defaultXrootdTimeoutSec            {3600};
std::string  const Configuration::defaultWorkerTechnology            {"TEST"};
size_t       const Configuration::defaultWorkerNumProcessingThreads  {1};
size_t       const Configuration::defaultWorkerNumFsProcessingThreads{1};
size_t       const Configuration::defaultWorkerFsBufferSizeBytes     {1048576};
std::string  const Configuration::defaultWorkerSvcHost               {"localhost"};
uint16_t     const Configuration::defaultWorkerSvcPort               {50000};
std::string  const Configuration::defaultWorkerFsHost                {"localhost"};
uint16_t     const Configuration::defaultWorkerFsPort                {50001};
std::string  const Configuration::defaultDataDir                     {"{worker}"};
std::string  const Configuration::defaultDatabaseTechnology          {"mysql"};
std::string  const Configuration::defaultDatabaseHost                {"localhost"};
uint16_t     const Configuration::defaultDatabasePort                {3306};
std::string  const Configuration::defaultDatabaseUser                {FileUtils::getEffectiveUser()};
std::string  const Configuration::defaultDatabasePassword            {""};
std::string  const Configuration::defaultDatabaseName                {"replica"};
unsigned int const Configuration::defaultJobSchedulerIvalSec         {1};
size_t       const Configuration::defaultReplicationLevel            {1};

void Configuration::translateDataDir(std::string&       dataDir,
                                     std::string const& workerName) {

    std::string::size_type const leftPos = dataDir.find('{');
    if (leftPos == std::string::npos) { return; }

    std::string::size_type const rightPos = dataDir.find('}');
    if (rightPos == std::string::npos) { return; }

    if (dataDir.substr (leftPos, rightPos - leftPos + 1) == "{worker}") {
        dataDir.replace(leftPos, rightPos - leftPos + 1, workerName);
    }
}

Configuration::Configuration()
    :   _requestBufferSizeBytes       (defaultRequestBufferSizeBytes),
        _retryTimeoutSec              (defaultRetryTimeoutSec),
        _controllerHttpPort           (defaultControllerHttpPort),
        _controllerHttpThreads        (defaultControllerHttpThreads),
        _controllerRequestTimeoutSec  (defaultControllerRequestTimeoutSec),
        _jobTimeoutSec                (defaultJobTimeoutSec),
        _jobHeartbeatTimeoutSec       (defaultJobHeartbeatTimeoutSec),
        _xrootdAutoNotify             (defaultXrootdAutoNotify),
        _xrootdHost                   (defaultXrootdHost),
        _xrootdPort                   (defaultXrootdPort),
        _xrootdTimeoutSec             (defaultXrootdTimeoutSec),
        _workerTechnology             (defaultWorkerTechnology),
        _workerNumProcessingThreads   (defaultWorkerNumProcessingThreads),
        _workerNumFsProcessingThreads (defaultWorkerNumFsProcessingThreads),
        _workerFsBufferSizeBytes      (defaultWorkerFsBufferSizeBytes),
        _databaseTechnology           (defaultDatabaseTechnology),
        _databaseHost                 (defaultDatabaseHost),
        _databasePort                 (defaultDatabasePort),
        _databaseUser                 (defaultDatabaseUser),
        _databasePassword             (defaultDatabasePassword),
        _databaseName                 (defaultDatabaseName),
        _jobSchedulerIvalSec          (defaultJobSchedulerIvalSec) {
}

std::string Configuration::context() const {
    static std::string const str = "CONFIG   ";
    return str;
}

std::vector<std::string> Configuration::workers(bool isEnabled,
                                                bool isReadOnly) const {
    std::vector<std::string> names;
    for (auto const& entry: _workerInfo) {
        auto const& name = entry.first;
        auto const& info = entry.second;
        if (isEnabled) {
            if (info.isEnabled and (isReadOnly == info.isReadOnly)) {
                names.push_back (name);
            }
        } else {
            if (not info.isEnabled) {
                names.push_back (name);
            }
        }
    }
    return names;
}

std::vector<std::string> Configuration::databaseFamilies() const {

    std::map<std::string, size_t> family2num;
    for (auto const& elem: _databaseInfo) {
        family2num[elem.second.family]++;
    }
    std::vector<std::string> families;
    for (auto const& elem: family2num) {
        families.emplace_back (elem.first);
    }
    return families;
}

bool Configuration::isKnownDatabaseFamily(std::string const& name) const {
    return _replicationLevel.count(name);
}

size_t Configuration::replicationLevel(std::string const& family) const {
    if (not _replicationLevel.count(family)) {
        throw std::invalid_argument(
                "Configuration::replicationLevel  unknown database family name: '" +
                family + "'");
    }
    return _replicationLevel.at(family);
}

std::vector<std::string> Configuration::databases(std::string const& family) const {

    if (not family.empty() and not _replicationLevel.count(family)) {
        throw std::invalid_argument(
                "Configuration::databases  unknown database family name: '" +
                family + "'");
    }
    std::vector<std::string> names;
    for (auto const& entry: _databaseInfo) {
        if (not family.empty() and (family != entry.second.family)) { continue; }
        names.push_back(entry.first);
    }
    return names;
}

bool Configuration::isKnownWorker(std::string const& name) const {
    return _workerInfo.count(name) > 0;
}

WorkerInfo const& Configuration::workerInfo(std::string const& name) const {
    if (not isKnownWorker(name)) {
        throw std::invalid_argument(
                "Configuration::workerInfo() uknown worker name '" + name + "'");
    }
    return _workerInfo.at(name);
}

bool Configuration::isKnownDatabase(std::string const& name) const {
    return _databaseInfo.count(name) > 0;
}

DatabaseInfo const& Configuration::databaseInfo(std::string const& name) const {
    if (not isKnownDatabase(name)) {
        throw std::invalid_argument(
                "Configuration::databaseInfo() uknown database name '" + name + "'");
    }
    return _databaseInfo.at(name);
}

void Configuration::dumpIntoLogger() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultRequestBufferSizeBytes:       " << defaultRequestBufferSizeBytes);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultRetryTimeoutSec:              " << defaultRetryTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultControllerHttpPort:           " << defaultControllerHttpPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultControllerHttpThreads:        " << defaultControllerHttpThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultControllerRequestTimeoutSec:  " << defaultControllerRequestTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultJobTimeoutSec:                " << defaultJobTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultJobHeartbeatTimeoutSec:       " << defaultJobHeartbeatTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultXrootdAutoNotify:             " << (defaultXrootdAutoNotify ? "true" : "false"));
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultXrootdHost:                   " << defaultXrootdHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultXrootdPort:                   " << defaultXrootdPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultXrootdTimeoutSec:             " << defaultXrootdTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerTechnology:             " << defaultWorkerTechnology);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerNumProcessingThreads:   " << defaultWorkerNumProcessingThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerNumFsProcessingThreads: " << defaultWorkerNumFsProcessingThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerFsBufferSizeBytes:      " << defaultWorkerFsBufferSizeBytes);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerSvcHost:                " << defaultWorkerSvcHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerSvcPort:                " << defaultWorkerSvcPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerFsHost:                 " << defaultWorkerFsHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultWorkerFsPort:                 " << defaultWorkerFsPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDataDir:                      " << defaultDataDir);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabaseTechnology:           " << defaultDatabaseTechnology);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabaseHost:                 " << defaultDatabaseHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabasePort:                 " << defaultDatabasePort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabaseUser:                 " << defaultDatabaseUser);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabasePassword:             " << "*****");
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultDatabaseName:                 " << defaultDatabaseName);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultJobSchedulerIvalSec:          " << defaultJobSchedulerIvalSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "defaultReplicationLevel:             " << defaultReplicationLevel);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_requestBufferSizeBytes:             " << _requestBufferSizeBytes);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_retryTimeoutSec:                    " << _retryTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_controllerHttpPort:                 " << _controllerHttpPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_controllerHttpThreads:              " << _controllerHttpThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_controllerRequestTimeoutSec:        " << _controllerRequestTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_jobTimeoutSec:                      " << _jobTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_jobHeartbeatTimeoutSec:             " << _jobHeartbeatTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_xrootdAutoNotify:                   " << (_xrootdAutoNotify ? "true" : "false"));
    LOGS(_log, LOG_LVL_DEBUG, context() << "_xrootdHost:                         " << _xrootdHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_xrootdPort:                         " << _xrootdPort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_xrootdTimeoutSec:                   " << _xrootdTimeoutSec);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_workerTechnology:                   " << _workerTechnology);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_workerNumProcessingThreads:         " << _workerNumProcessingThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_workerNumFsProcessingThreads:       " << _workerNumFsProcessingThreads);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_workerFsBufferSizeBytes:            " << _workerFsBufferSizeBytes);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databaseTechnology:                 " << _databaseTechnology);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databaseHost:                       " << _databaseHost);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databasePort:                       " << _databasePort);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databaseUser:                       " << _databaseUser);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databasePassword:                   " << "*****");
    LOGS(_log, LOG_LVL_DEBUG, context() << "_databaseName:                       " << _databaseName);
    LOGS(_log, LOG_LVL_DEBUG, context() << "_jobSchedulerIvalSec:                " << _jobSchedulerIvalSec);
    for (auto const& elem: _workerInfo) {
        LOGS(_log, LOG_LVL_DEBUG, context() << elem.second);
    }
    for (auto const& elem: _databaseInfo) {
        LOGS(_log, LOG_LVL_DEBUG, context() << elem.second);
    }
    for (auto const& elem: _replicationLevel) {
        LOGS(_log, LOG_LVL_DEBUG, context()
             << "replicationLevel["<< elem.first << "]: " << elem.second);
    }
}

}}} // namespace lsst::qserv::replica
