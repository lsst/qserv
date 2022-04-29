/*
 * LSST Data Management System
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
#include <tuple>

// Qserv headers
#include "replica/FileUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseMySQL");

}  // namespace

namespace lsst { namespace qserv { namespace replica { namespace database { namespace mysql {

/////////////////////////////////////////////////////
//                ConnectionParams                 //
/////////////////////////////////////////////////////

ConnectionParams::ConnectionParams()
        : host("localhost"),
          port(3306),
          user(replica::FileUtils::getEffectiveUser()),
          password(""),
          database("") {}

ConnectionParams::ConnectionParams(string const& host_, uint16_t port_, string const& user_,
                                   string const& password_, string const& database_)
        : host(host_), port(port_), user(user_), password(password_), database(database_) {}

ConnectionParams ConnectionParams::parse(string const& params, string const& defaultHost,
                                         uint16_t defaultPort, string const& defaultUser,
                                         string const& defaultPassword) {
    string const context = "ConnectionParams::" + string(__func__) + "  ";

    regex re("^[ ]*mysql://([^:]+)?(:([^:]?.*[^@]?))?@([^:^/]+)?(:([0-9]+))?(/([^ ]+))[ ]*$",
             regex::extended);
    smatch match;

    if (not regex_search(params, match, re)) {
        throw invalid_argument(context + "incorrect syntax of the encoded connection parameters string");
    }
    if (match.size() != 9) {
        throw runtime_error(context + "problem with the regular expression");
    }

    ConnectionParams connectionParams;

    string const user = match[1].str();
    connectionParams.user = user.empty() ? defaultUser : user;

    string const password = match[3].str();
    connectionParams.password = password.empty() ? defaultPassword : password;

    string const host = match[4].str();
    connectionParams.host = host.empty() ? defaultHost : host;

    string const port = match[6].str();
    connectionParams.port = port.empty() ? defaultPort : (uint16_t)stoul(port);

    // no default option for the database
    connectionParams.database = match[8].str();
    if (connectionParams.database.empty()) {
        throw invalid_argument(context + "database name not found in the encoded parameters string");
    }

    LOGS(_log, LOG_LVL_DEBUG, context << connectionParams);

    return connectionParams;
}

string ConnectionParams::toString(bool showPassword) const {
    return string("mysql://") + user + ":" + (showPassword ? password : string("xxxxxx")) + "@" + host + ":" +
           to_string(port) + "/" + database;
}

bool ConnectionParams::operator==(ConnectionParams const& rhs) const {
    return tie(host, port, user, password, database) ==
           tie(rhs.host, rhs.port, rhs.user, rhs.password, rhs.database);
}

ostream& operator<<(ostream& os, ConnectionParams const& params) {
    os << "DatabaseMySQL::ConnectionParams "
       << "(" << params.toString() << ")";
    return os;
}

//////////////////////////////////////////////////
//                DoNotProcess                  //
//////////////////////////////////////////////////

DoNotProcess::DoNotProcess(string const& name_) : name(name_) {}

/////////////////////////////////////////////
//                Keyword                  //
/////////////////////////////////////////////

Keyword const Keyword::SQL_NULL{"NULL"};

Keyword::Keyword(string const& name_) : DoNotProcess(name_) {}

/////////////////////////////////////////////
//                Function                 //
/////////////////////////////////////////////

Function const Function::LAST_INSERT_ID{"LAST_INSERT_ID()"};

Function::Function(string const& name_) : DoNotProcess(name_) {}

}}}}}  // namespace lsst::qserv::replica::database::mysql
