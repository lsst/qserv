// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 AURA/LSST.
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
#include "css/CssConfig.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.css.CssConfig");

}

namespace lsst {
namespace qserv {
namespace css {

CssConfig::CssConfig(util::ConfigStore const& configStore)
    try : _technology(configStore.get("technology")),
      _data(configStore.get("data")),
      _file(configStore.get("file")),
      _mySqlConfig(configStore.get("username"),
           configStore.get("password"),
           configStore.get("hostname"),
           configStore.getInt("port"),
           configStore.get("socket"),
           configStore.get("database")) {

    if (_technology.empty()) {
        std::string msg = "\"technology\" does not exist in configuration map";
        LOGS(_log, LOG_LVL_ERROR, msg);
        throw ConfigError(msg);
    }

    if (not _data.empty() and  not _file.empty()) {
        std::string msg = "\"data\"  and \"file\" keys are mutually exclusive";
        LOGS(_log, LOG_LVL_ERROR, msg);
        throw ConfigError(msg);
    }
} catch (util::ConfigStoreError const& e) {
    throw ConfigError(e.what());
}

std::ostream& operator<<(std::ostream &out, CssConfig const& cssConfig) {
    out << "[ technology=" << cssConfig._technology << ", data=" << cssConfig._data
        << ", file=" << cssConfig._file << ", mysql_configuration=" << cssConfig._mySqlConfig <<"]";
    return out;
}

}}} // namespace lsst::qserv::css


