/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// Class header
#include "ccontrol/UserQueryType.h"

// System headers

// Third-party headers
#include "boost/regex.hpp"
#include "boost/algorithm/string/case_conv.hpp"

// LSST headers
#include "query/FromList.h"
#include "query/SelectStmt.h"
#include "query/SelectList.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"

namespace {

// regex for SELECT *
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _selectRe(R"(^select\s+.+$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for SHOW [FULL] PROCESSLIST
// if FULL is present then group 1 is non-empty
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _showProcessListRe(R"(^show\s+(full\s+)?processlist$)",
                                boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for SUBMIT ...
// group 1 is the query without SUBMIT prefix
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _submitRe(R"(^submit\s+(.+)$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for EXPLAIN [FORMAT=JSON|TRADITIONAL] ...
// group 1 (optional) is the requested output format, group 2 is the stripped query
boost::regex _explainRe(R"(^explain\s+(?:format\s*=\s*(json|traditional)\s+)?(.+)$)",
                        boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for SELECT * FROM QSERV_RESULT(12345)
// group 1 is the query ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _selectResultRe(R"(^select\s+\*\s+from\s+qserv_result\s*\(\s*(\d+)\s*\)$)",
                             boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for KILL [QUERY|CONNECTION] 12345
// group 1 is the thread ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _killRe(R"(^kill\s+(?:QUERY\s+|CONNECTION\s+)?(\d+)\s*$)",
                     boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for CANCEL 12345
// group 1 is the query ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _cancelRe(R"(^cancel\s+(\d+)\s*$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for CALL
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _callRe(R"(^call\s+.+$)",
                     boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex for SET
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _setRe(R"(^set\s+.+$)", boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex extracting `SET GLOBAL <varName> = <value>`. GLOBAL is required, but the
// value is intentionally not validated (left to the caller).
boost::regex _setGlobalRe(R"(^set\s+global\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\S.*?)\s*;?\s*$)",
                          boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

// regex extracting the argument of `CALL QSERV_RESULT_DELETE(<queryId>)`
boost::regex _resultDeleteRe(R"(^call\s+qserv_result_delete\s*\(\s*(\d+)\s*\)\s*;?\s*$)",
                             boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);

}  // namespace

namespace lsst::qserv::ccontrol {

/// Returns true if query is regular SELECT (not isSelectResult())
bool UserQueryType::isSelect(std::string const& query) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _selectRe);
    if (match) {
        if (boost::regex_match(query, sm, _selectResultRe)) {
            match = false;
        }
    }
    return match;
}

/// Returns true if query is SHOW [FULL] PROCESSLIST
bool UserQueryType::isShowProcessList(std::string const& query, bool& full) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _showProcessListRe);
    if (match) {
        full = sm.length(1) != 0;
    }
    return match;
}

/// Returns true if table name refers to PROCESSLIST table
bool UserQueryType::isProcessListTable(std::string const& dbName, std::string const& tblName) {
    return boost::to_upper_copy(dbName) == "INFORMATION_SCHEMA" &&
           boost::to_upper_copy(tblName) == "PROCESSLIST";
}

/// Returns true if table name refers to QUERIES table
bool UserQueryType::isQueriesTable(std::string const& dbName, std::string const& tblName) {
    return boost::to_upper_copy(dbName) == "INFORMATION_SCHEMA" && boost::to_upper_copy(tblName) == "QUERIES";
}

/// Returns true if query is SUBMIT ...
bool UserQueryType::isSubmit(std::string const& query, std::string& stripped) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _submitRe);
    if (match) {
        stripped = sm.str(1);
    }
    return match;
}

/// Returns true if query is EXPLAIN [FORMAT=JSON|TRADITIONAL] ...
bool UserQueryType::isExplain(std::string const& query, std::string& stripped, bool& jsonFormat) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _explainRe);
    if (match) {
        jsonFormat = boost::to_lower_copy(sm.str(1)) == "json";
        stripped = sm.str(2);
    }
    return match;
}

/// Returns true if query is SELECT * FROM QSERV_RESULT(...)
bool UserQueryType::isSelectResult(std::string const& query, QueryId& queryId) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _selectResultRe);
    if (match) {
        queryId = std::stoull(sm.str(1));
    }
    return match;
}

// Returns true if query is KILL [QUERY|CONNECTION] NNN
bool UserQueryType::isKill(std::string const& query, int& threadId) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _killRe);
    if (match) {
        threadId = std::stoi(sm.str(1));
    }
    return match;
}

// Returns true if query is CANCEL NNN
bool UserQueryType::isCancel(std::string const& query, QueryId& queryId) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _cancelRe);
    if (match) {
        queryId = std::stoull(sm.str(1));
    }
    return match;
}

bool UserQueryType::isCall(std::string const& query) { return boost::regex_match(query, _callRe); }

bool UserQueryType::isResultDelete(std::string const& query, std::string& queryId) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _resultDeleteRe);
    if (match) {
        queryId = sm.str(1);
    }
    return match;
}

bool UserQueryType::isSimpleCountStar(std::shared_ptr<query::SelectStmt> const& stmt, std::string& spelling) {
    if (stmt->hasWhereClause() || stmt->hasOrderBy() || stmt->hasGroupBy() || stmt->hasHaving()) {
        return false;
    }
    auto& selectList = stmt->getSelectList();
    auto valueExprVec = selectList.getValueExprList();
    if (valueExprVec == nullptr) return false;
    if (valueExprVec->size() != 1) return false;
    if (not(*valueExprVec)[0]->isCountStar(&spelling)) return false;
    auto& fromList = stmt->getFromList();
    auto& tableRefVec = fromList.getTableRefList();
    if (tableRefVec.size() != 1) return false;
    if (not tableRefVec[0]->isSimple())  // do not allow JOIN
        return false;

    return true;
}

/// Returns true if query is SET
bool UserQueryType::isSet(std::string const& query) {
    boost::smatch sm;
    return boost::regex_match(query, sm, _setRe);
}

bool UserQueryType::isSetGlobal(std::string const& query, std::string& varName, std::string& varValue) {
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _setGlobalRe);
    if (match) {
        varName = sm.str(1);
        varValue = sm.str(2);
    }
    return match;
}

}  // namespace lsst::qserv::ccontrol
