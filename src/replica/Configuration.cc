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
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/ConfigurationBase.h"
#include "replica/ConfigurationTypes.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

// These parameters are allowed to be changed, and they are set globally
// for an application (process).
bool         Configuration::_databaseAllowReconnect        = true;
unsigned int Configuration::_databaseConnectTimeoutSec     = 3600;
unsigned int Configuration::_databaseMaxReconnects         = 1;
unsigned int Configuration::_databaseTransactionTimeoutSec = 3600;
string       Configuration::_qservMasterDatabasePassword   = "";
string       Configuration::_qservWorkerDatabasePassword   = "";
bool         Configuration::_xrootdAllowReconnect          = true;
unsigned int Configuration::_xrootdConnectTimeoutSec       = 3600;


string Configuration::setQservMasterDatabasePassword(string const& newPassword) {
    string result = newPassword;
    swap(result, _qservMasterDatabasePassword);
    return result;
}


string Configuration::setQservWorkerDatabasePassword(string const& newPassword) {
    string result = newPassword;
    swap(result, _qservWorkerDatabasePassword);
    return result;
}


bool Configuration::setDatabaseAllowReconnect(bool value) {
    swap(value, _databaseAllowReconnect);
    return value;
}


unsigned int Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseConnectTimeoutSec);
    return value;
}


unsigned int Configuration::setDatabaseMaxReconnects(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseMaxReconnects);
    return value;
}


unsigned int Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseTransactionTimeoutSec);
    return value;
}


bool Configuration::setXrootdAllowReconnect(bool value) {
    swap(value, _xrootdAllowReconnect);
    return value;

}


unsigned int Configuration::setXrootdConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _xrootdConnectTimeoutSec);
    return value;

}


json Configuration::toJson(Configuration::Ptr const& config) {

    json configJson;

    // General parameters

    ConfigurationGeneralParams general;
    configJson["general"] = general.toJson(config);

    // Workers

    json workersJson;
    for (auto&& worker: config->allWorkers()) {
        auto const wi = config->workerInfo(worker);
        workersJson.push_back(wi.toJson());
    }
    configJson["workers"] = workersJson;

    // Database families, databases, and tables

    json familiesJson;
    for (auto&& family: config->databaseFamilies()) {
        auto const fi = config->databaseFamilyInfo(family);
        json familyJson = fi.toJson();
        bool const allDatabases = true;
        for (auto&& database: config->databases(family, allDatabases)) {
            auto const di = config->databaseInfo(database);
            familyJson["databases"].push_back(di.toJson());
        }
        familiesJson.push_back(familyJson);
    }
    configJson["families"] = familiesJson;

    return configJson;
}


Configuration::Ptr Configuration::load(string const& configUrl) {
    return Ptr(new Configuration(ConfigurationBase::load(configUrl)));
}


Configuration::Ptr Configuration::load(map<string, string> const& kvMap) {
    return Ptr(new Configuration(ConfigurationBase::load(kvMap)));
}


void Configuration::reload() {
    util::Lock lock(_mtx, context(__func__));
    if (_impl->prefix() != "map") {
        bool const showPassword = true;
        _impl = ConfigurationBase::load(_impl->configUrl(showPassword));
    }
}


void Configuration::reload(string const& configUrl) {
    util::Lock lock(_mtx, context(__func__));
    _impl = ConfigurationBase::load(configUrl);
}


void Configuration::reload(std::map<std::string, std::string> const& kvMap) {
    util::Lock lock(_mtx, context(__func__));
    if (_impl->prefix() != "map") _impl = ConfigurationBase::load(kvMap);
}

    
string Configuration::prefix() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->prefix();
}


string Configuration::configUrl(bool showPassword) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->configUrl(showPassword);    
}

vector<string> Configuration::workers(bool isEnabled, bool isReadOnly) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->workers(isEnabled, isReadOnly);
}


vector<string> Configuration::allWorkers() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->allWorkers();
}


size_t Configuration::requestBufferSizeBytes() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->requestBufferSizeBytes();
}


void Configuration::setRequestBufferSizeBytes(size_t val,
                                              bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setRequestBufferSizeBytes(val, updatePersistentState);
}


unsigned int Configuration::retryTimeoutSec() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->retryTimeoutSec();
}


void Configuration::setRetryTimeoutSec(unsigned int val,
                                       bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setRetryTimeoutSec(val, updatePersistentState);
}


size_t Configuration::controllerThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->controllerThreads();
}


void Configuration::setControllerThreads(size_t val,
                                         bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setControllerThreads(val, updatePersistentState);
}


uint16_t Configuration::controllerHttpPort() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->controllerHttpPort();
}


void Configuration::setControllerHttpPort(uint16_t val,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setControllerHttpPort(val, updatePersistentState);
}


size_t Configuration::controllerHttpThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->controllerHttpThreads();
}


void Configuration::setControllerHttpThreads(size_t val,
                                             bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setControllerHttpThreads(val, updatePersistentState);
}


unsigned int Configuration::controllerRequestTimeoutSec() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->controllerRequestTimeoutSec();
}


string Configuration::controllerEmptyChunksDir() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->controllerEmptyChunksDir();
}


void Configuration::setControllerRequestTimeoutSec(unsigned int val,
                                                   bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setControllerRequestTimeoutSec(val, updatePersistentState);
}


unsigned int Configuration::jobTimeoutSec() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->jobTimeoutSec();
}


void Configuration::setJobTimeoutSec(unsigned int val,
                                     bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setJobTimeoutSec(val, updatePersistentState);
}


unsigned int Configuration::jobHeartbeatTimeoutSec() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->jobHeartbeatTimeoutSec();
}


void Configuration::setJobHeartbeatTimeoutSec(unsigned int val,
                                              bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setJobHeartbeatTimeoutSec(val, updatePersistentState);
}


bool Configuration::xrootdAutoNotify() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->xrootdAutoNotify();
}


void Configuration::setXrootdAutoNotify(bool val,
                                        bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setXrootdAutoNotify(val, updatePersistentState);
}


string Configuration::xrootdHost() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->xrootdHost();
}


void Configuration::setXrootdHost(string const& val,
                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setXrootdHost(val, updatePersistentState);
}


uint16_t Configuration::xrootdPort() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->xrootdPort();
}


void Configuration::setXrootdPort(uint16_t val,
                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setXrootdPort(val, updatePersistentState);
}


unsigned int Configuration::xrootdTimeoutSec() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->xrootdTimeoutSec();
}


void Configuration::setXrootdTimeoutSec(unsigned int val,
                                        bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setXrootdTimeoutSec(val, updatePersistentState);
}


string Configuration::databaseTechnology() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseTechnology();
}


string Configuration::databaseHost() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseHost();
}


uint16_t Configuration::databasePort() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databasePort();
}


string Configuration::databaseUser() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseUser();
}


string Configuration::databasePassword() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databasePassword();
}


string Configuration::databaseName() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseName();
}


size_t Configuration::databaseServicesPoolSize() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseServicesPoolSize();
}


void Configuration::setDatabaseServicesPoolSize(size_t val,
                                                bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setDatabaseServicesPoolSize(val, updatePersistentState);
}


string Configuration::qservMasterDatabaseHost() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabaseHost();
}


uint16_t Configuration::qservMasterDatabasePort() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabasePort();
}


string Configuration::qservMasterDatabaseUser() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabaseUser();
}


string Configuration::qservMasterDatabaseName() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabaseName();
}


size_t Configuration::qservMasterDatabaseServicesPoolSize() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabaseServicesPoolSize();
}


string Configuration::qservMasterDatabaseTmpDir() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->qservMasterDatabaseTmpDir();
}


vector<string> Configuration::databaseFamilies() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseFamilies();
}


bool Configuration::isKnownDatabaseFamily(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->isKnownDatabaseFamily(name);
}


DatabaseFamilyInfo Configuration::databaseFamilyInfo(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseFamilyInfo(name);
}


DatabaseFamilyInfo Configuration::addDatabaseFamily(DatabaseFamilyInfo const& info) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->addDatabaseFamily(info);
}


void Configuration::deleteDatabaseFamily(string const& name) {
    util::Lock lock(_mtx, context(__func__));
    _impl->deleteDatabaseFamily(name);
}


size_t Configuration::replicationLevel(string const& family) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->replicationLevel(family);
}


vector<string> Configuration::databases(string const& family, bool allDatabases, bool isPublished) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databases(family, allDatabases, isPublished);
}


bool Configuration::isKnownDatabase(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->isKnownDatabase(name);
}


DatabaseInfo Configuration::databaseInfo(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->databaseInfo(name);
}


DatabaseInfo Configuration::addDatabase(DatabaseInfo const& info) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->addDatabase(info);
}


DatabaseInfo Configuration::publishDatabase(string const& name) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->publishDatabase(name);
}


DatabaseInfo Configuration::unPublishDatabase(string const& name) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->unPublishDatabase(name);
}


void Configuration::deleteDatabase(string const& name) {
    util::Lock lock(_mtx, context(__func__));
    _impl->deleteDatabase(name);
}


DatabaseInfo Configuration::addTable(string const& database, string const& table, bool isPartitioned,
                                     list<SqlColDef> const& columns, bool isDirectorTable,
                                     string const& directorTableKey, string const& chunkIdColName,
                                     string const& subChunkIdColName, string const& latitudeColName,
                                     string const& longitudeColName) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->addTable(database, table, isPartitioned, columns,
                           isDirectorTable, directorTableKey,
                           chunkIdColName, subChunkIdColName, latitudeColName, longitudeColName);
}


DatabaseInfo Configuration::deleteTable(string const& database, string const& table) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->deleteTable(database, table);
}


bool Configuration::isKnownWorker(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->isKnownWorker(name);
}


WorkerInfo Configuration::workerInfo(string const& name) const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->workerInfo(name);
}


string Configuration::workerTechnology() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->workerTechnology();
}


void Configuration::setWorkerTechnology(string const& val,
                                        bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setWorkerTechnology(val, updatePersistentState);
}


void Configuration::addWorker(WorkerInfo const& info) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->addWorker(info);
}


void Configuration::deleteWorker(string const& name) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->deleteWorker(name);
}


WorkerInfo Configuration::disableWorker(string const& name,
                                        bool disable,
                                        bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->disableWorker(name, disable, updatePersistentState);
}


WorkerInfo Configuration::setWorkerReadOnly(string const& name,
                                            bool readOnly,
                                            bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerReadOnly(name, readOnly, updatePersistentState);
}


WorkerInfo Configuration::setWorkerSvcHost(string const& name,
                                           string const& host,
                                           bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerSvcHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerSvcPort(string const& name,
                                           uint16_t port,
                                           bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerSvcPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerFsHost(string const& name,
                                          string const& host,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerFsHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerFsPort(string const& name,
                                          uint16_t port,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerFsPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerDataDir(string const& name,
                                           string const& dataDir,
                                           bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerDataDir(name, dataDir, updatePersistentState);
}


WorkerInfo Configuration::setWorkerDbHost(string const& name,
                                          string const& host,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerDbHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerDbPort(string const& name,
                                          uint16_t port,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerDbPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerDbUser(string const& name,
                                          string const& user,
                                          bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerDbUser(name, user, updatePersistentState);
}


WorkerInfo Configuration::setWorkerLoaderHost(string const& name,
                                              string const& host,
                                              bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerLoaderHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerLoaderPort(string const& name,
                                              uint16_t port,
                                              bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerLoaderPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerLoaderTmpDir(string const& name,
                                                string const& tmpDir,
                                                bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerLoaderTmpDir(name, tmpDir, updatePersistentState);
}


WorkerInfo Configuration::setWorkerExporterHost(string const& name,
                                                string const& host,
                                                bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerExporterHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerExporterPort(string const& name,
                                                uint16_t port,
                                                bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerExporterPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerExporterTmpDir(string const& name,
                                                  string const& tmpDir,
                                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerExporterTmpDir(name, tmpDir, updatePersistentState);
}


WorkerInfo Configuration::setWorkerHttpLoaderHost(string const& name,
                                                  string const& host,
                                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerHttpLoaderHost(name, host, updatePersistentState);
}


WorkerInfo Configuration::setWorkerHttpLoaderPort(string const& name,
                                                  uint16_t port,
                                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerHttpLoaderPort(name, port, updatePersistentState);
}


WorkerInfo Configuration::setWorkerHttpLoaderTmpDir(string const& name,
                                                    string const& tmpDir,
                                                    bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    return _impl->setWorkerHttpLoaderTmpDir(name, tmpDir, updatePersistentState);
}


size_t Configuration::workerNumProcessingThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->workerNumProcessingThreads();
}


void Configuration::setWorkerNumProcessingThreads(size_t val,
                                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setWorkerNumProcessingThreads(val, updatePersistentState);
}


size_t Configuration::fsNumProcessingThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->fsNumProcessingThreads();
}


void Configuration::setFsNumProcessingThreads(size_t val,
                                              bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setFsNumProcessingThreads(val, updatePersistentState);
}


size_t Configuration::workerFsBufferSizeBytes() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->workerFsBufferSizeBytes();
}


void Configuration::setWorkerFsBufferSizeBytes(size_t val,
                                               bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setWorkerFsBufferSizeBytes(val, updatePersistentState);
}


size_t Configuration::loaderNumProcessingThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->loaderNumProcessingThreads();
}


void Configuration::setLoaderNumProcessingThreads(size_t val,
                                                  bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setLoaderNumProcessingThreads(val, updatePersistentState);
}


size_t Configuration::exporterNumProcessingThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->exporterNumProcessingThreads();
}


void Configuration::setExporterNumProcessingThreads(size_t val,
                                                    bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setExporterNumProcessingThreads(val, updatePersistentState);
}


size_t Configuration::httpLoaderNumProcessingThreads() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->httpLoaderNumProcessingThreads();
}


void Configuration::setHttpLoaderNumProcessingThreads(size_t val,
                                                      bool updatePersistentState) {
    util::Lock lock(_mtx, context(__func__));
    _impl->setHttpLoaderNumProcessingThreads(val, updatePersistentState);
}

void Configuration::dumpIntoLogger() const {
    util::Lock lock(_mtx, context(__func__));
    _impl->dumpIntoLogger();
}


string Configuration::asString() const {
    util::Lock lock(_mtx, context(__func__));
    return _impl->asString();
}

}}} // namespace lsst::qserv::replica
