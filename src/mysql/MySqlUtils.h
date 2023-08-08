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
#ifndef LSST_QSERV_MYSQL_MYSQLUTILS_H
#define LSST_QSERV_MYSQL_MYSQLUTILS_H

// System headers
#include <stdexcept>

// Third-party headers
#include "nlohmann/json.hpp"

/// Forward declarations.
namespace lsst::qserv::mysql {
class MySqlConfig;
}  // namespace lsst::qserv::mysql

/// This header declarations.
namespace lsst::qserv::mysql {

/**
 * Class MySqlQueryError represents exceptions to be throw on specific errors
 * detected when attempting to execute the queries.
 */
class MySqlQueryError : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Class MySqlUtils is the utility class providing a collection of useful queries reporting
 * small result sets.
 * @note Each tool of the collection does its own connection handling (opening/etc.).
 */
class MySqlUtils {
public:
    /**
     * Report info on the on-going queries using 'SHOW [FULL] PROCESSLIST'.
     * @param A scope of the operaton depends on the user credentials privided
     *   in the configuration object. Normally, a subset of queries which belong
     *   to the specified user will be reported.
     * @param config Configuration parameters of the MySQL connector.
     * @param full The optional modifier which (if set) allows seeing the full text
     *   of the queries.
     * @return A collection of queries encoded as the JSON object. Please, see the code
     *   for further details on the schema of the object.
     * @throws MySqlQueryError on errors detected during query execution/processing.
     */
    static nlohmann::json processList(MySqlConfig const& config, bool full = false);
};

}  // namespace lsst::qserv::mysql

#endif  // LSST_QSERV_MYSQL_MYSQLUTILS_H
