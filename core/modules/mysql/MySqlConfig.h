// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2014 LSST Corporation.
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

#ifndef LSST_QSERV_MYSQL_MYSQLCONFIG_H
#define LSST_QSERV_MYSQL_MYSQLCONFIG_H

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace mysql {

/// class MySqlConfig : Value class for configuring the MySQL connection
class MySqlConfig {
public:
    MySqlConfig() : port(0) {}
    MySqlConfig(std::string const& username, std::string const& password,
                std::string const& hostname,
                unsigned int const port,
                std::string const& socket,
                std::string const& dbName = "");
    MySqlConfig(std::string const& username, std::string const& password,
                std::string const& socket, std::string const& dbName = "");

    bool checkConnection() const;

    bool isValid() const { return !username.empty(); }

    /** Overload output operator for current class
     *
     * @param out
     * @param mysqlConfig
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, MySqlConfig const& mysqlConfig);

    std::string username;
    std::string password;
    std::string hostname;
    unsigned int port;
    std::string socket;
    std::string dbName;

    std::string toString() const;
};

}}} // namespace lsst::qserv::mysql

#endif // LSST_QSERV_MYSQL_MYSQLCONFIG_H
