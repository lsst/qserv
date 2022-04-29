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

#ifndef LSST_QSERV_SQL_SQLCONFIG_H
#define LSST_QSERV_SQL_SQLCONFIG_H

// Qserv headers
#include "mysql/MySqlConfig.h"  // Our goal is to remove MySqlConfig and replace the class with a URL string
                                // that contains the same information.

namespace lsst { namespace qserv { namespace sql {

class SqlConfig {
public:
    enum Type { MYSQL, MOCK };

    typedef std::map<std::string, std::map<std::string, std::vector<std::string>>> MockDbTableColumns;

    SqlConfig(mysql::MySqlConfig const& cfg) : mySqlConfig(cfg), type(MYSQL) {}

    SqlConfig(MockDbTableColumns const& columnInfo) : dbTableColumns(columnInfo), type(MOCK) {}

    SqlConfig(Type type) : type(type) {}

    /// config for a MySqlConnection, for use if type == MYSQL
    mysql::MySqlConfig mySqlConfig;

    /// config for a MockSql connection, for use if type == MOCK
    // MockDbTableColumns are used when configuring a MockSql.
    // These should get replaced by an sqlite database, when we have an SqliteConnection class.
    MockDbTableColumns dbTableColumns;

    Type type;

private:
    SqlConfig() = default;
};

}}}  // namespace lsst::qserv::sql

#endif  // LSST_QSERV_SQL_SQLCONFIG_H
