// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2015 AURA/LSST.
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
#include "mysql/MySqlConfig.h"

// System headers
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "sql/SqlConnection.h"

namespace {

    LOG_LOGGER getLogger() {
        static LOG_LOGGER logger = LOG_GET("lsst.qserv.mysql.MySqlConfig");
        return logger;
    }


} // anonymous

namespace lsst {
namespace qserv {
namespace mysql {

MySqlConfig::MySqlConfig(std::string const& username,
                         std::string const& password,
                         std::string const& hostname,
                         unsigned int const port,
                         std::string const& socket,
                         std::string const& dbName)
    : username(username), password(password), hostname(hostname), port(port),
      socket(socket), dbName(dbName) {

    if ((hostname.empty() or port == 0) and socket.empty()) {
        LOGS(getLogger(), LOG_LVL_FATAL, "MySQL hostname:port and socket both undefined");
        throw std::invalid_argument("MySQL hostname:port and socket both undefined" );
    }
}

MySqlConfig::MySqlConfig(std::string const& username, std::string const& password,
                         std::string const& socket, std::string const& dbName)
    : username(username), password(password), port(0), socket(socket), dbName(dbName) {
    if (socket.empty()) {
        LOGS(getLogger(), LOG_LVL_FATAL, "MySQL socket undefined");
        throw std::invalid_argument("MySQL socket undefined");
    }
}

bool MySqlConfig::checkConnection() const {
    lsst::qserv::sql::SqlConnection scn(*this);
    lsst::qserv::sql::SqlErrorObject eo;
    if (scn.connectToDb(eo)) {
        LOGS(getLogger(), LOG_LVL_DEBUG, "Successful MySQL connection check: " << *this);
        return true;
    } else {
        LOGS(getLogger(), LOG_LVL_WARN, "Unsuccessful MySQL connection check: " << *this);
        return false;
    }
}

std::ostream& operator<<(std::ostream &out, MySqlConfig const& mysqlConfig) {
    out << "[host=" << mysqlConfig.hostname << ", port=" << mysqlConfig.port
        << ", user=" << mysqlConfig.username << ", password=" << mysqlConfig.password
        << ", db=" << mysqlConfig.dbName << ", socket=" << mysqlConfig.socket << "]";
    return out;
}

std::string MySqlConfig::toString() const {
    std::ostringstream oss;
    oss << *this;
    return oss.str();
}

}}} // namespace lsst::qserv::mysql
