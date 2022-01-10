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
#include "czar/CzarConfig.h"

// System headers

// Third party headers
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.CzarConfig");

void QservLogger(struct timeval const& mtime,
                 unsigned long         tID,
                 const char*           msg,
                 int                   mlen) {

    static log4cxx::spi::LocationInfo xrdLoc("client", "<xrdssi>", 0);
    static LOG_LOGGER myLog = LOG_GET("lsst.qserv.xrdssi.msgs");

    if (myLog.isInfoEnabled()) {
        while (mlen && msg[mlen-1] == '\n') --mlen;  // strip all trailing newlines
        std::string theMsg(msg, mlen);
        lsst::log::Log::MDC("LWP", std::to_string(tID));
        myLog.logMsg(log4cxx::Level::getInfo(), xrdLoc, theMsg);
    }
}

bool dummy = XrdSsiLogger::SetMCB(QservLogger, XrdSsiLogger::mcbClient);
}

namespace lsst {
namespace qserv {
namespace czar {

CzarConfig::CzarConfig(util::ConfigStore const& configStore)
    : _mySqlResultConfig(configStore.get("resultdb.user", "qsmaster"),
            configStore.getRequired("resultdb.passwd"),
            configStore.getRequired("resultdb.host"), configStore.getInt("resultdb.port"),
            configStore.getRequired("resultdb.unix_socket"),
            configStore.get("resultdb.db","qservResult")),
      _logConfig(configStore.get("log.logConfig")),
      _maxTableSizeMB(configStore.getInt("resultdb.maxtablesize_mb", 5001)),
      _maxSqlConnectionAttempts(configStore.getInt("resultdb.maxsqlconnectionattempts", 10)),
      _resultEngine(configStore.get("resultdb.engine", "myisam")),
      _resultMaxConnections(configStore.getInt("resultdb.maxconnections", 40)),
      _oldestResultKeptDays(configStore.getInt("resultdb.oldestResultKeptDays", 30)),
      _cssConfigMap(configStore.getSectionConfigMap("css")),
      _mySqlQmetaConfig(configStore.get("qmeta.user", "qsmaster"),
                        configStore.get("qmeta.passwd"),
                        configStore.get("qmeta.host"),
                        configStore.getInt("qmeta.port", 3306),
                        configStore.get("qmeta.unix_socket"),
                        configStore.get("qmeta.db", "qservMeta")),
      _mySqlQstatusDataConfig(configStore.get("qstatus.user", "qsmaster"),
                              configStore.get("qstatus.passwd"),
                              configStore.get("qstatus.host"),
                              configStore.getInt("qstatus.port", 3306),
                              configStore.get("qstatus.unix_socket"),
                              configStore.get("qstatus.db", "qservStatusData")),
      _xrootdFrontendUrl(configStore.get("frontend.xrootd", "localhost:1094")),
      _emptyChunkPath(configStore.get("partitioner.emptyChunkPath", ".")),
      _interactiveChunkLimit(configStore.getInt("tuning.interactiveChunkLimit", 10)),
      _xrootdCBThreadsMax(configStore.getInt("tuning.xrootdCBThreadsMax", 500)),
      _xrootdCBThreadsInit(configStore.getInt("tuning.xrootdCBThreadsInit", 50)),
      _xrootdSpread(configStore.getInt("tuning.xrootdSpread", 4)),
      _qMetaSecsBetweenChunkCompletionUpdates(configStore.getInt(
                                               "tuning.qMetaSecsBetweenChunkCompletionUpdates", 60)),
      _queryDistributionTestVer(configStore.getInt("tuning.queryDistributionTestVer", 0)),
      _qdispPoolSize(configStore.getInt("qdisppool.poolSize", 1000)),
      _qdispMaxPriority(configStore.getInt("qdisppool.largestPriority", 2)),
      _qdispVectRunSizes(configStore.get("qdisppool.vectRunSizes", "50:50:50:50")),
      _qdispVectMinRunningSizes(configStore.get("qdisppool.vectMinRunningSizes", "0:1:3:3")),
      _qReqPseudoFifoMaxRunning(configStore.getInt("qdisppool.qReqPseudoFifoMaxRunning", 300)) {
}

std::ostream& operator<<(std::ostream &out, CzarConfig const& czarConfig) {
    out << "[cssConfigMap=" << util::printable(czarConfig._cssConfigMap) <<
           ", emptyChunkPath=" << czarConfig._emptyChunkPath <<
           ", logConfig=" << czarConfig._logConfig <<
           ", mySqlQmetaConfig=" << czarConfig._mySqlQmetaConfig <<
           ", mySqlQStatusDataConfig=" << czarConfig._mySqlQstatusDataConfig <<
           ", mySqlResultConfig=" << czarConfig._mySqlResultConfig <<
           ", xrootdFrontendUrl=" << czarConfig._xrootdFrontendUrl <<
           "]";

    return out;
}

}}} // namespace lsst::qserv::czar


