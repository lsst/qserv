/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/DatabaseMySQLTypes.h"

// System headers
#include <regex>
#include <sstream>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/FileUtils.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseMySQL");

}   // namespace

namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/////////////////////////////////////////////////////
//                ConnectionParams                 //
/////////////////////////////////////////////////////

ConnectionParams::ConnectionParams()
    :   host("localhost"),
        port(3306),
        user(replica::FileUtils::getEffectiveUser()),
        password(""),
        database("") {
}

ConnectionParams ConnectionParams::parse(std::string const& params,
                                         std::string const& defaultHost,
                                         uint16_t           defaultPort,
                                         std::string const& defaultUser,
                                         std::string const& defaultPassword) {

    std::string const context = "ConnectionParams::parse  ";

    std::regex re("^mysql://([^:]+)?(:([^:]?.*[^@]?))?@([^:^/]+)?(:([0-9]+))?(/([^/]+))?$",
                  std::regex::extended);
    std::smatch match;
    if (not std::regex_search(params, match, re)) {
        throw std::invalid_argument(context + "incorrect syntax of the encoded connection parameters string");
    }
    if (match.size() != 9) {
        throw std::runtime_error(context + "problem with the regular expression");
    }

    ConnectionParams connectionParams;

    std::string const user = match[1].str();
    connectionParams.user  = user.empty() ? defaultUser : user;

    std::string const password = match[3].str();
    connectionParams.password = password.empty() ?  defaultPassword : password;

    std::string const host = match[4].str();
    connectionParams.host  = host.empty() ? defaultHost : host;

    std::string const port = match[6].str();
    connectionParams.port  = port.empty() ?  defaultPort : (uint16_t)std::stoul(port);

    // no default option for the database
    connectionParams.database = match[8].str();
    if (connectionParams.database.empty()) {
        throw std::invalid_argument(
                context + "database name not found in the encoded parameters string");
    }

    LOGS(_log, LOG_LVL_DEBUG, context << connectionParams);


    return connectionParams;
}

std::string ConnectionParams::toString() const {
    return
        std::string("mysql://") + user + ":xxxxxx@" + host + ":" + std::to_string(port) + "/" + database;
}

std::ostream& operator<<(std::ostream& os, ConnectionParams const& params) {
    os  << "DatabaseMySQL::ConnectionParams " << "(" << params.toString() << ")";
    return os;
}

//////////////////////////////////////////////////
//                DoNotProcess                  //
//////////////////////////////////////////////////

DoNotProcess::DoNotProcess(std::string const& name_)
    :   name(name_) {
}

/////////////////////////////////////////////
//                Keyword                  //
/////////////////////////////////////////////

Keyword const Keyword::SQL_NULL {"NULL"};

Keyword::Keyword(std::string const& name_)
    :   DoNotProcess(name_) {
}

/////////////////////////////////////////////
//                Function                 //
/////////////////////////////////////////////

Function const Function::LAST_INSERT_ID {"LAST_INSERT_ID()"};

Function::Function(std::string const& name_)
    :   DoNotProcess(name_) {
}

}}}}} // namespace lsst::qserv::replica::database::mysql
