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

#ifndef LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H
#define LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H

// System headers
#include <memory>

// Forward declarations
namespace lsst::qserv {
namespace mysql {
class MySqlConfig;
}
namespace sql {
class SqlConnection;
class SqlConfig;
}  // namespace sql
}  // namespace lsst::qserv

namespace lsst::qserv::sql {

class SqlConnectionFactory {
public:
    /**
     * @brief Make a new SqlConnection object from an SqlConfig
     */
    static std::shared_ptr<SqlConnection> make(SqlConfig const& cfg);

    /**
     * @brief Make a new SqlConnection object from a mysql::MySqlConfig
     *
     * This is deprecated and should not be added to new code, callers should prefer the function that takes
     * an SqlConfig.
     */
    static std::shared_ptr<SqlConnection> make(mysql::MySqlConfig const& cfg);
};

}  // namespace lsst::qserv::sql

#endif  // LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H
