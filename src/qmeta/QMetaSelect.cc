/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
#include "qmeta/QMetaSelect.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "Exceptions.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMetaSelect");

}

namespace lsst::qserv::qmeta {

QMetaSelect::QMetaSelect(mysql::MySqlConfig const& mysqlConf)
        : _conn(sql::SqlConnectionFactory::make(mysqlConf)) {}

std::unique_ptr<sql::SqlResults> QMetaSelect::select(std::string const& query) {
    sql::SqlErrorObject errObj;
    std::unique_ptr<sql::SqlResults> results(new sql::SqlResults);
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    std::lock_guard<std::mutex> const lock(_connMtx);
    if (!_conn->runQuery(query, *results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    return results;
}

}  // namespace lsst::qserv::qmeta
