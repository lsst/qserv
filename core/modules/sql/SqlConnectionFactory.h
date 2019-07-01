/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H
#define LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H


// System headers
#include <memory>


// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}
namespace sql {
    class SqlConnection;
    class SqlConfig;
}}}


namespace lsst {
namespace qserv {
namespace sql {

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


}}}


#endif // LSST_QSERV_SQL_SQLCONNECTIONFACTORY_H
