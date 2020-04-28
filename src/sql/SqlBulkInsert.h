/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_SQL_SQLBULKINSERT_H
#define LSST_QSERV_SQL_SQLBULKINSERT_H

// System headers
#include <vector>
#include <string>

// Third-party headers

// Qserv headers
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

namespace lsst {
namespace qserv {
namespace sql {

/// @addtogroup sql

/**
 *  @ingroup sql
 *
 *  @brief Class implementing bulk insert memory buffering.
 *
 *  Builds in-memory INSERT statement using "bulk insert" syntax:
 *
 *    INSERT INTO Table (columns) VALUES (row1), (row2), ...
 *
 *  If buffer size becomes too large it sends statement to server and starts
 *  building next one. Client has to call flush() method after last row is
 *  added.
 */

class SqlBulkInsert  {
public:

    /**
     *  Instantiate inserter object.
     *
     *  @param conn:  database connection
     *  @param table: Tnble name
     *  @param columns: List of column names
     */
    SqlBulkInsert(SqlConnection* conn,
                  std::string const& table,
                  std::vector<std::string> const& columns);

    /**
     *  Insert one more row
     *
     *  Takes a list of column values for a single row. String values
     *  must be properly quoted and escaped.
     *
     *  @param values: List of values for table columns
     *  @param[out] errObj: Error details
     *  @returns True on success, false on error
     */
    bool addRow(std::vector<std::string> const& values, SqlErrorObject& errObj);

    /**
     *  Force memory buffer flush.
     *
     *  This method has to be called at least once after all rows have been added.
     *  Destructor does NOT call this method.
     *
     *  @param[out] errObj: Error details
     *  @returns True on success, false on error
     */
    bool flush(SqlErrorObject& errObj);

protected:

private:

    // Data members
    SqlConnection* _conn;
    unsigned long _maxSize;     ///< Max. allowed buffer size
    std::string _insert;   ///< INSERT ... (columns) VALUES
    std::string _buffer;   ///< Buffer for query

    SqlBulkInsert(SqlBulkInsert const&) = delete;
    SqlBulkInsert& operator=(SqlBulkInsert const&) = delete;
};

}}} // namespace lsst::qserv::sql

#endif // LSST_QSERV_SQL_SQLBULKINSERT_H
