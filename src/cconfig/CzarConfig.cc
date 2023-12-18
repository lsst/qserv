// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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
#include "cconfig/CzarConfig.h"

// System headers
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.cconfig.CzarConfig");

void QservLogger(struct timeval const& mtime, unsigned long tID, const char* msg, int mlen) {
    static log4cxx::spi::LocationInfo xrdLoc("client", "<xrdssi>", 0);
    static LOG_LOGGER myLog = LOG_GET("lsst.qserv.xrdssi.msgs");

    if (myLog.isInfoEnabled()) {
        while (mlen && msg[mlen - 1] == '\n') --mlen;  // strip all trailing newlines
        std::string theMsg(msg, mlen);
        lsst::log::Log::MDC("LWP", std::to_string(tID));
        myLog.logMsg(log4cxx::Level::getInfo(), xrdLoc, theMsg);
    }
}

bool dummy = XrdSsiLogger::SetMCB(QservLogger, XrdSsiLogger::mcbClient);
}  // namespace

namespace lsst::qserv::cconfig {

std::mutex CzarConfig::_mtxOnInstance;

std::shared_ptr<CzarConfig> CzarConfig::_instance;

std::shared_ptr<CzarConfig> CzarConfig::create(std::string const& configFileName) {
    std::lock_guard<std::mutex> const lock(_mtxOnInstance);
    if (_instance == nullptr) {
        _instance = std::shared_ptr<CzarConfig>(new CzarConfig(util::ConfigStore(configFileName)));
    }
    return _instance;
}

std::shared_ptr<CzarConfig> CzarConfig::instance() {
    std::lock_guard<std::mutex> const lock(_mtxOnInstance);
    if (_instance == nullptr) {
        throw std::logic_error("CzarConfig::" + std::string(__func__) + ": instance has not been created.");
    }
    return _instance;
}

CzarConfig::CzarConfig(util::ConfigStore const& configStore)
        : _mySqlResultConfig(configStore.get("resultdb.user", "qsmaster"),
                             configStore.getRequired("resultdb.passwd"),
                             configStore.getRequired("resultdb.host"), configStore.getInt("resultdb.port"),
                             configStore.getRequired("resultdb.unix_socket"),
                             configStore.get("resultdb.db", "qservResult")),
          _maxTableSizeMB(configStore.getInt("resultdb.maxtablesize_mb", 5001)),
          _maxSqlConnectionAttempts(configStore.getInt("resultdb.maxsqlconnectionattempts", 10)),
          _resultEngine(configStore.get("resultdb.engine", "myisam")),
          _resultMaxConnections(configStore.getInt("resultdb.maxconnections", 40)),
          _oldestResultKeptDays(configStore.getInt("resultdb.oldestResultKeptDays", 30)),
          _cssConfigMap(configStore.getSectionConfigMap("css")),
          _mySqlQmetaConfig(configStore.get("qmeta.user", "qsmaster"), configStore.get("qmeta.passwd"),
                            configStore.get("qmeta.host"), configStore.getInt("qmeta.port", 3306),
                            configStore.get("qmeta.unix_socket"), configStore.get("qmeta.db", "qservMeta")),
          _mySqlQstatusDataConfig(
                  configStore.get("qstatus.user", "qsmaster"), configStore.get("qstatus.passwd"),
                  configStore.get("qstatus.host"), configStore.getInt("qstatus.port", 3306),
                  configStore.get("qstatus.unix_socket"), configStore.get("qstatus.db", "qservStatusData")),
          _xrootdFrontendUrl(configStore.get("frontend.xrootd", "localhost:1094")),
          _emptyChunkPath(configStore.get("partitioner.emptyChunkPath", ".")),
          _interactiveChunkLimit(configStore.getInt("tuning.interactiveChunkLimit", 10)),
          _xrootdCBThreadsMax(configStore.getInt("tuning.xrootdCBThreadsMax", 500)),
          _xrootdCBThreadsInit(configStore.getInt("tuning.xrootdCBThreadsInit", 50)),
          _xrootdSpread(configStore.getInt("tuning.xrootdSpread", 4)),
          _qMetaSecsBetweenChunkCompletionUpdates(
                  configStore.getInt("tuning.qMetaSecsBetweenChunkCompletionUpdates", 60)),
          _maxMsgSourceStore(configStore.getInt("qmeta.maxMsgSourceStore", 3)),
          _queryDistributionTestVer(configStore.getInt("tuning.queryDistributionTestVer", 0)),
          _qdispPoolSize(configStore.getInt("qdisppool.poolSize", 1000)),
          _qdispMaxPriority(configStore.getInt("qdisppool.largestPriority", 2)),
          _qdispVectRunSizes(configStore.get("qdisppool.vectRunSizes", "50:50:50:50")),
          _qdispVectMinRunningSizes(configStore.get("qdisppool.vectMinRunningSizes", "0:1:3:3")),
          _qReqPseudoFifoMaxRunning(configStore.getInt("qdisppool.qReqPseudoFifoMaxRunning", 300)),
          _notifyWorkersOnQueryFinish(configStore.getInt("tuning.notifyWorkersOnQueryFinish", 1)),
          _notifyWorkersOnCzarRestart(configStore.getInt("tuning.notifyWorkersOnCzarRestart", 1)),
          _czarStatsUpdateIvalSec(configStore.getInt("tuning.czarStatsUpdateIvalSec", 1)),
          _czarStatsRetainPeriodSec(configStore.getInt("tuning.czarStatsRetainPeriodSec", 24 * 3600)),
          _replicationInstanceId(configStore.get("replication.instance_id", "")),
          _replicationAuthKey(configStore.get("replication.auth_key", "")),
          _replicationAdminAuthKey(configStore.get("replication.admin_auth_key", "")),
          _replicationRegistryHost(configStore.get("replication.registry_host", "")),
          _replicationRegistryPort(configStore.getInt("replication.registry_port", 0)),
          _replicationRegistryHearbeatIvalSec(
                  configStore.getInt("replication.registry_heartbeat_ival_sec", 1)),
          _replicationHttpPort(configStore.getInt("replication.http_port", 0)),
          _replicationNumHttpThreads(configStore.getInt("replication.num_http_threads", 2)) {
    if (_replicationRegistryHost.empty()) {
        throw std::invalid_argument("CzarConfig::" + std::string(__func__) +
                                    ": 'replication.registry_host' is not set.");
    }
    if (_replicationRegistryPort == 0) {
        throw std::invalid_argument("CzarConfig::" + std::string(__func__) +
                                    ": 'replication.registry_port' number can't be 0.");
    }
    if (_replicationRegistryHearbeatIvalSec == 0) {
        throw std::invalid_argument("CzarConfig::" + std::string(__func__) +
                                    ": 'replication.registry_heartbeat_ival_sec' can't be 0.");
    }
    if (_replicationNumHttpThreads == 0) {
        throw std::invalid_argument("CzarConfig::" + std::string(__func__) +
                                    ": 'replication.num_http_threads' can't be 0.");
    }

    // Cache the cached version of the configuration in the JSON format. The JSON object
    // contains two collections of parameters: the "input" ones that were passed into
    // the contructor, and the "actual" ones that were expected by the current implementation
    // of Czar.
    _jsonConfig =
            nlohmann::json::object({{"input", configStore.toJson()}, {"actual", nlohmann::json::object()}});

    // Note that actual collection may contain parameters not mentioned in
    // the input configuration.
    nlohmann::json& actualJsonConfig = _jsonConfig["actual"];
    actualJsonConfig["resultdb"] =
            nlohmann::json::object({{"user", _mySqlResultConfig.username},
                                    {"passwd", "xxxxx"},
                                    {"host", _mySqlResultConfig.hostname},
                                    {"port", std::to_string(_mySqlResultConfig.port)},
                                    {"unix_socket", _mySqlResultConfig.socket},
                                    {"db", _mySqlResultConfig.dbName},
                                    {"maxtablesize_mb", std::to_string(_maxTableSizeMB)},
                                    {"maxsqlconnectionattempts", std::to_string(_maxSqlConnectionAttempts)},
                                    {"engine", _resultEngine},
                                    {"maxconnections", std::to_string(_resultMaxConnections)},
                                    {"oldestResultKeptDays", std::to_string(_oldestResultKeptDays)}});
    actualJsonConfig["css"] = _cssConfigMap;
    actualJsonConfig["qmeta"] =
            nlohmann::json::object({{"user", _mySqlQmetaConfig.username},
                                    {"passwd", "xxxxx"},
                                    {"host", _mySqlQmetaConfig.hostname},
                                    {"port", std::to_string(_mySqlQmetaConfig.port)},
                                    {"unix_socket", _mySqlQmetaConfig.socket},
                                    {"db", _mySqlQmetaConfig.dbName},
                                    {"maxMsgSourceStore", std::to_string(_maxMsgSourceStore)}});
    actualJsonConfig["qstatus"] =
            nlohmann::json::object({{"user", _mySqlQstatusDataConfig.username},
                                    {"passwd", "xxxxx"},
                                    {"host", _mySqlQstatusDataConfig.hostname},
                                    {"port", std::to_string(_mySqlQstatusDataConfig.port)},
                                    {"unix_socket", _mySqlQstatusDataConfig.socket},
                                    {"db", _mySqlQstatusDataConfig.dbName}});
    actualJsonConfig["frontend"] = nlohmann::json::object({{"xrootd", _xrootdFrontendUrl}});
    actualJsonConfig["partitioner"] = nlohmann::json::object({{"emptyChunkPath", _emptyChunkPath}});
    actualJsonConfig["tuning"] = nlohmann::json::object(
            {{"interactiveChunkLimit", std::to_string(_interactiveChunkLimit)},
             {"xrootdCBThreadsMax", std::to_string(_xrootdCBThreadsMax)},
             {"xrootdCBThreadsInit", std::to_string(_xrootdCBThreadsInit)},
             {"xrootdSpread", std::to_string(_xrootdSpread)},
             {"qMetaSecsBetweenChunkCompletionUpdates",
              std::to_string(_qMetaSecsBetweenChunkCompletionUpdates)},
             {"queryDistributionTestVer", std::to_string(_queryDistributionTestVer)},
             {"notifyWorkersOnQueryFinish", std::to_string(_notifyWorkersOnQueryFinish)},
             {"notifyWorkersOnCzarRestart", std::to_string(_notifyWorkersOnCzarRestart)},
             {"czarStatsUpdateIvalSec", std::to_string(_czarStatsUpdateIvalSec)},
             {"czarStatsRetainPeriodSec", std::to_string(_czarStatsRetainPeriodSec)}});
    actualJsonConfig["qdisppool"] = nlohmann::json::object({
            {"poolSize", std::to_string(_qdispPoolSize)},
            {"largestPriority", std::to_string(_qdispMaxPriority)},
            {"vectRunSizes", _qdispVectRunSizes},
            {"vectMinRunningSizes", _qdispVectMinRunningSizes},
            {"qReqPseudoFifoMaxRunning", std::to_string(_qReqPseudoFifoMaxRunning)},
    });
    actualJsonConfig["replication"] = nlohmann::json::object(
            {{"instance_id", _replicationInstanceId},
             {"auth_key", "xxxxx"},
             {"admin_auth_key", "xxxxx"},
             {"registry_host", _replicationRegistryHost},
             {"registry_port", std::to_string(_replicationRegistryPort)},
             {"registry_heartbeat_ival_sec", std::to_string(_replicationRegistryHearbeatIvalSec)},
             {"http_port", std::to_string(_replicationHttpPort)},
             {"num_http_threads", std::to_string(_replicationNumHttpThreads)}});
}

void CzarConfig::setReplicationHttpPort(uint16_t port) {
    if (port == 0) {
        throw std::invalid_argument("CzarConfig::" + std::string(__func__) + ": port number can't be 0.");
    }
    _replicationHttpPort = port;
    // Update the relevant section of the JSON-ified configuration.
    _jsonConfig["actual"]["replication"]["http_port"] = std::to_string(_replicationHttpPort);
}

std::string CzarConfig::id() { return "default"; }

std::ostream& operator<<(std::ostream& out, CzarConfig const& czarConfig) {
    out << czarConfig._jsonConfig.dump();
    return out;
}

}  // namespace lsst::qserv::cconfig
