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

// Class header
#include "SqlConnectionFactory.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "sql/MySqlConnection.h"
#include "sql/SqlConfig.h"
#include "sql/SqlConnection.h"


namespace lsst {
namespace qserv {
namespace sql {


std::shared_ptr<SqlConnection> SqlConnectionFactory::make(SqlConfig const& cfg) {
    return std::shared_ptr<sql::MySqlConnection>(new sql::MySqlConnection(cfg.mySqlConfig));
}


std::shared_ptr<SqlConnection> SqlConnectionFactory::make(mysql::MySqlConfig const& cfg) {
    return make(SqlConfig(cfg));
}



}}} // namespace lsst::qserv::sql
