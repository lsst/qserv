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

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/IterableFormatter.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.CzarConfig");

}

namespace lsst {
namespace qserv {
namespace czar {

CzarConfig::CzarConfig(util::ConfigStore const& configStore)
    : _mySqlResultConfig(configStore.get("resultdb.user", "qsmaster"),
            configStore.getRequired("resultdb.passwd"),
            configStore.getRequired("resultdb.host"), configStore.getInt("resultdb.port"),
            configStore.getRequired("resultdb.unix_socket"), configStore.get("resultdb.db","qservResult")),
      _logConfig(configStore.get("log.logConfig")),
      _cssConfigMap(configStore.getSectionConfigMap("css")),
      _mySqlQmetaConfig(configStore.get( "qmeta.user", "qsmaster"),
                        configStore.get("qmeta.passwd"),
                        configStore.get("qmeta.host"),
                        configStore.getInt("qmeta.port", 3306),
                        configStore.get("qmeta.unix_socket"),
                        configStore.get("qmeta.db", "qservMeta")),
       _xrootdFrontendUrl(configStore.get("frontend.xrootd", "localhost:1094")),
       _emptyChunkPath(configStore.get("partitioner.emptyChunkPath", ".")) {
}

std::ostream& operator<<(std::ostream &out, CzarConfig const& czarConfig) {
    out << "[cssConfigMap=" << util::printable(czarConfig._cssConfigMap) <<
           ", emptyChunkPath=" << czarConfig._emptyChunkPath <<
           ", logConfig=" << czarConfig._logConfig <<
           ", mySqlQmetaConfig=" << czarConfig._mySqlQmetaConfig <<
           ", mySqlResultConfig=" << czarConfig._mySqlResultConfig <<
           ", xrootdFrontendUrl=" << czarConfig._xrootdFrontendUrl <<
           "]";

    return out;
}

}}} // namespace lsst::qserv::czar


