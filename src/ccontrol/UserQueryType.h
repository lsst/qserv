/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
#ifndef LSST_QSERV_CCONTROL_USERQUERYTYPE_H
#define LSST_QSERV_CCONTROL_USERQUERYTYPE_H

// System headers
#include <cstdint>
#include <memory>
#include <string>

// Third-party headers

// Qserv headers
#include "global/intTypes.h"

namespace lsst::qserv::query {
class SelectStmt;
}

namespace lsst::qserv::ccontrol {

/// @addtogroup ccontrol

/**
 *  @ingroup ccontrol
 *
 *  @brief Helper class for parsing queries and determining their types.
 */

class UserQueryType {
public:
    /// Returns true if query is regular SELECT (not isSelectResult())
    static bool isSelect(std::string const& query);

    /// Returns true if query is FLUSH QSERV_CHUNKS_CACHE [FOR database]
    static bool isFlushChunksCache(std::string const& query, std::string& dbName);

    /**
     *  Returns true if query is SHOW [FULL] PROCESSLIST
     *
     *  @param[in] query:  SQL query string
     *  @param[out] full:  Set to true if FULL is in query
     *  @returns True if query is indeed a SHOW PROCESSLIST
     */
    static bool isShowProcessList(std::string const& query, bool& full);

    /**
     *  Returns true if database/table name refers to PROCESSLIST table in
     *  INFORMATION_SCHEMA pseudo-database.
     */
    static bool isProcessListTable(std::string const& dbName, std::string const& tblName);

    /**
     *  Returns true if database/table name refers to QUERIES table in
     *  INFORMATION_SCHEMA pseudo-database.
     */
    static bool isQueriesTable(std::string const& dbName, std::string const& tblName);

    /**
     *  Returns true if query is SUBMIT ..., returns query without "SUBMIT"
     *  in `stripped` string.
     */
    static bool isSubmit(std::string const& query, std::string& stripped);

    /**
     *  Returns true if query is SELECT * FROM QSERV_RESULT(...), returns
     *  query ID in `queryId` argument.
     */
    static bool isSelectResult(std::string const& query, QueryId& queryId);

    /**
     *  Returns true if query is KILL [QUERY|CONNECTION] NNN, returns
     *  thread ID in `threadId` argument.
     */
    static bool isKill(std::string const& query, int& threadId);

    /**
     *  Returns true if query is CANCEL NNN, returns
     *  query ID in `queryId` argument.
     */
    static bool isCancel(std::string const& query, QueryId& queryId);

    /**
     *  Returns true if query is CALL ...
     */
    static bool isCall(std::string const& query);

    /**
     *
     * @param stmt the statement to check
     * @param spelling if not nullptr will set the string to the spelling of
     *                 count (may be lower case, mixed case, or upper case)
     *
     * Returns true if the select statement is SELECT COUNT(*) FROM <database>
     */
    static bool isSimpleCountStar(std::shared_ptr<query::SelectStmt> const& stmt, std::string& spelling);

    /// Returns true if query is SET (for variable assignment)
    static bool isSet(std::string const& query);
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_USERQUERYTYPE_H
