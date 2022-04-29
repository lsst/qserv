// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#include "SqlConnectionFactory.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "sql/MockSql.h"
#include "sql/MySqlConnection.h"
#include "sql/SqlConfig.h"
#include "sql/SqlConnection.h"

namespace lsst::qserv::sql {

std::shared_ptr<SqlConnection> SqlConnectionFactory::make(SqlConfig const& cfg) {
    if (SqlConfig::MOCK == cfg.type) {
        return std::make_shared<MockSql>(cfg.dbTableColumns);
    }
    return std::shared_ptr<sql::MySqlConnection>(new sql::MySqlConnection(cfg.mySqlConfig));
}

std::shared_ptr<SqlConnection> SqlConnectionFactory::make(mysql::MySqlConfig const& cfg) {
    return make(SqlConfig(cfg));
}

}  // namespace lsst::qserv::sql
