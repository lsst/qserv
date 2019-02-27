/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */



// Class header
#include "qmeta/QStatus.h"

// System headers

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/Exceptions.h"
#include "qmeta/QStatusMysql.h"
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QStatus");

}

namespace lsst {
namespace qserv {
namespace qmeta {

QStatus::Ptr QStatus::createFromConfig(std::map<std::string, std::string> const& config) {
    LOGS(_log, LOG_LVL_DEBUG, "QStatus::createFromConfig");

    util::ConfigStore configStore(config);
    std::string technology;

    try {
        technology = configStore.getRequired("technology");
    } catch (util::KeyNotFoundError const& e) {
    	std::string emsg(std::string("QStatus technology not found in config ") + e.what());
        LOGS(_log, LOG_LVL_DEBUG, emsg);
        throw ConfigError(ERR_LOC, emsg);
    }
    if (technology == "mysql") {
        try {
            // extract all optional values from map
            mysql::MySqlConfig mysqlConfig(configStore.get("username"),
               configStore.get("password"),
               configStore.get("hostname"),
               configStore.getInt("port"),
               configStore.get("socket"),
               configStore.get("database"));

                LOGS(_log, LOG_LVL_DEBUG, "Create QMeta instance with mysql store");
                return std::make_shared<QStatusMysql>(mysqlConfig);
        } catch (util::ConfigStoreError const& exc) {
        	std::string emsg(std::string("QStatus Exception while creating MySQL configuration: ")
        	                 + exc.what());
            LOGS(_log, LOG_LVL_DEBUG, emsg);
            throw ConfigError(ERR_LOC, emsg);
        }
    } else {
    	std::string emsg(std::string("QStatus - Unexpected value of \"technology\" key: ") + technology);
        LOGS(_log, LOG_LVL_DEBUG, emsg);
        throw ConfigError(ERR_LOC, emsg);
    }
}


}}} // namespace lsst::qserv::qmeta

