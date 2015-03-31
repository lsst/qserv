// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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
// SqlResults class implementation file.

// Class header
#include "sql/SqlResults.h"

// System headers
#include <sstream>

namespace lsst {
namespace qserv {
namespace sql {

void
SqlResults::freeResults() {
    int i, s = _results.size();
    for (i=0 ; i<s ; ++i) {
        mysql_free_result(_results[i]);
    }
    _results.clear();
}

void
SqlResults::addResult(MYSQL_RES* r) {
    if ( _discardImmediately ) {
        mysql_free_result(r);
    } else {
        _results.push_back(r);
    }
}

bool
SqlResults::extractFirstColumn(std::vector<std::string>& ret,
                               SqlErrorObject& errObj) {
    int i, s = _results.size();
    for (i=0 ; i<s ; ++i) {
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(_results[i])) != NULL) {
            ret.push_back(row[0]);
        }
        mysql_free_result(_results[i]);
    }
    _results.clear();
    return true;
}

bool
SqlResults::extractFirst2Columns(std::vector<std::string>& col1,
                                 std::vector<std::string>& col2,
                                 SqlErrorObject& errObj) {
    int i, s = _results.size();
    for (i=0 ; i<s ; ++i) {
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(_results[i])) != NULL) {
            col1.push_back(row[0]);
            col2.push_back(row[1]);
        }
        mysql_free_result(_results[i]);
    }
    _results.clear();
    return true;
}

bool
SqlResults::extractFirst3Columns(std::vector<std::string>& col1,
                                 std::vector<std::string>& col2,
                                 std::vector<std::string>& col3,
                                 SqlErrorObject& errObj) {
    int i, s = _results.size();
    for (i=0 ; i<s ; ++i) {
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(_results[i])) != NULL) {
            col1.push_back(row[0]);
            col2.push_back(row[1]);
            col3.push_back(row[2]);
        }
        mysql_free_result(_results[i]);
    }
    _results.clear();
    return true;
}

bool
SqlResults::extractFirst4Columns(std::vector<std::string>& col1,
                                 std::vector<std::string>& col2,
                                 std::vector<std::string>& col3,
                                 std::vector<std::string>& col4,
                                 SqlErrorObject& errObj) {
    int i, s = _results.size();
    for (i=0 ; i<s ; ++i) {
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(_results[i])) != NULL) {
            col1.push_back(row[0]);
            col2.push_back(row[1]);
            col3.push_back(row[2]);
            col4.push_back(row[3]);
        }
        mysql_free_result(_results[i]);
    }
    _results.clear();
    return true;
}

bool
SqlResults::extractFirstValue(std::string& ret, SqlErrorObject& errObj) {
    if (_results.size() != 1) {
        std::stringstream ss;
        ss << "Expecting one row, found " << _results.size() << " results"
           << std::endl;
        return errObj.addErrMsg(ss.str());
    }
    MYSQL_ROW row = mysql_fetch_row(_results[0]);
    if (!row) {
        return errObj.addErrMsg("Expecting one row, found no rows");
    }
    ret = (row[0]);
    freeResults();
    return true;
}

}}} // namespace lsst::qserv::sql
