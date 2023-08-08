// -*- LSST-C++ -*-
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
#include "mysql/MySqlUtils.h"

// System headers
#include <string>

// Third party headers
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#include <mysql/errmsg.h>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"

using namespace std;
using json = nlohmann::json;

namespace {

string errInfo(lsst::qserv::mysql::MySqlConnection const& conn) {
    return "errno: " + to_string(conn.getErrno()) + ", error: " + conn.getError();
}

}  // anonymous namespace

namespace lsst::qserv::mysql {

json MySqlUtils::processList(MySqlConfig const& config, bool full) {
    string const context = "MySqlUtils::" + string(__func__);
    string const query = "SHOW" + string(full ? " FULL" : "") + " PROCESSLIST";

    MySqlConnection conn(config);
    if (!conn.connect()) {
        string const err = context + " failed to connect to the worker database, " + ::errInfo(conn);
        throw MySqlQueryError(err);
    }
    if (!conn.queryUnbuffered(query)) {
        string const err = "failed to execute the query: '" + query + "', " + ::errInfo(conn);
        throw MySqlQueryError(err);
    }
    json result;
    result["queries"] = json::object({{"columns", json::array()}, {"rows", json::array()}});
    int const numFields = conn.getResultFieldCount();
    if (numFields > 0) {
        result["queries"]["columns"] = conn.getColumnNames();
        auto& rows = result["queries"]["rows"];
        MYSQL_RES* mysqlResult = conn.getResult();
        while (true) {
            MYSQL_ROW mysqlRow = mysql_fetch_row(mysqlResult);
            if (!mysqlRow) {
                if (0 == conn.getErrno()) {
                    // End of iteration if no specific error was reported.
                    break;
                }
                string const err =
                        context + " failed to fetch next row for query: '" + query + "', " + ::errInfo(conn);
                throw MySqlQueryError(err);
            }
            size_t const* lengths = mysql_fetch_lengths(mysqlResult);
            json row = json::array();
            for (int i = 0; i < numFields; i++) {
                // Report the empty string for SQL NULL.
                auto const length = lengths[i];
                row.push_back(length == 0 ? string() : string(mysqlRow[i], length));
            }
            rows.push_back(row);
        }
    }
    return result;
}

}  // namespace lsst::qserv::mysql
