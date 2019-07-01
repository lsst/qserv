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


#ifndef LSST_QSERV_SQL_SQLCONFIG_H
#define LSST_QSERV_SQL_SQLCONFIG_H


// Qserv headers
#include "mysql/MySqlConfig.h" // todo decouple this file



namespace lsst {
namespace qserv {
namespace sql {


class SqlConfig {
public:
    SqlConfig() = default;

    SqlConfig(mysql::MySqlConfig const& cfg) : mySqlConfig(cfg) {}

    mysql::MySqlConfig mySqlConfig;
};


}}}


#endif // LSST_QSERV_SQL_SQLCONFIG_H
