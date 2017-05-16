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

// System headers

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "Exceptions.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMetaSelect");

}

namespace lsst {
namespace qserv {
namespace qmeta {

// Constructors
QMetaSelect::QMetaSelect(mysql::MySqlConfig const& mysqlConf)
  : _conn(mysqlConf) {
}

// Destructor
QMetaSelect::~QMetaSelect() {
}

std::unique_ptr<sql::SqlResults>
QMetaSelect::select(std::string const& query) {

    // run query
    sql::SqlErrorObject errObj;
    std::unique_ptr<sql::SqlResults> results(new sql::SqlResults);
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn.runQuery(query, *results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw SqlError(ERR_LOC, errObj);
    }

    return results;
}

}}} // namespace lsst::qserv::qmeta
