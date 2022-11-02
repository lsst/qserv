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
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQLUTILS_H
#define LSST_QSERV_REPLICA_DATABASEMYSQLUTILS_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/DatabaseMySQLRow.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class Connection;
class Warning;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica::database::mysql {

namespace detail {
bool selectSingleValueImpl(std::shared_ptr<Connection> const& conn, std::string const& query,
                           std::function<bool(Row&)> const& onEachRow, bool noMoreThanOne = true);
}  // namespace detail

/**
 * The convenience method for executing queries from which a single value
 * would be extracted.
 *
 * @note By default, the method requires a result set to have 0 or 1 rows.
 *   Otherwise an exception be thrown. This requirement can be relaxed by setting
 *   a value of the optional parameter noMoreThanOne=false. In that case a value
 *   of the very first row will be extracted.
 *
 * @param conn The MySQL connection that was used to execute the last statement.
 * @param query A query to be executed.
 * @param val A value to be set (unless the field contains NULL).
 * @param colIdx The optional zero-based index of a column from which to extract a value.
 * @param noMoreThanOne A flag (if set) alowing more than one row in the result set.
 *
 * @return 'true' if the value is not NULL.
 *
 * @throws logic_error If the query is not supposed to return any result set,
 *   or if the result set has nore than 1 row (unless noMoreThanOne = false).
 * @throws EmptyResultSetError If a result set is empty.
 * @throws InvalidTypeError If the conversion to a proposed type will fail.
 */
template <typename T>
inline bool selectSingleValue(std::shared_ptr<Connection> const& conn, std::string const& query, T& val,
                              size_t colIdx = 0, bool noMoreThanOne = true) {
    auto const onEachRow = [&](Row& row) -> bool { return row.get(colIdx, val); };
    return detail::selectSingleValueImpl(conn, query, onEachRow, noMoreThanOne);
}

/// A variant for the above-defined single value selector based on the name of a column.
template <typename T>
inline bool selectSingleValue(std::shared_ptr<Connection> const& conn, std::string const& query, T& val,
                              std::string const& colName, bool noMoreThanOne = true) {
    auto const onEachRow = [&](Row& row) -> bool { return row.get(colName, val); };
    return detail::selectSingleValueImpl(conn, query, onEachRow, noMoreThanOne);
}

}  // namespace lsst::qserv::replica::database::mysql

#endif  // LSST_QSERV_REPLICA_DATABASEMYSQLUTILS_H
