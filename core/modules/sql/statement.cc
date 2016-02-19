// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#include "sql/statement.h"

// System headers
#include <sstream>

// Third-party headers


// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "sql/Schema.h"

namespace {

    LOG_LOGGER getLogger() {
        static LOG_LOGGER logger = LOG_GET("lsst.qserv.sql.statement");
        return logger;
    }


} // anonymous

namespace lsst {
namespace qserv {
namespace sql {

std::string formCreateTable(std::string const& table, sql::Schema const& s) {
    if (table.empty()) {
        throw Bug("sql/statement.cc: No table name for CREATE TABLE");
    }
    std::ostringstream os;
    os << "CREATE TABLE " << table << " (";
    ColumnsIter b, i, e;
    for(i=b=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        if (i != b) {
            os << ",\n";
        }
        os << *i;
    }
    os << ")";
    return os.str();
}

std::shared_ptr<InsertColumnVector> newInsertColumnVector(Schema const& s) {
    std::shared_ptr<InsertColumnVector> icv = std::make_shared<InsertColumnVector>();
    ColumnsIter b, i, e;
    for(i=b=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        InsertColumn ic;
        ic.column = i->name;
        if (i->colType.sqlType.find("BLOB") != std::string::npos) {
            std::ostringstream os;
            os << "blobtmp" << icv->size();
            ic.hexColumn = os.str();
        }
        icv->push_back(ic);
    }
    return icv;
}

std::string formLoadInfile(std::string const& table,
                           std::string const& virtFile) {
    auto sql = "LOAD DATA LOCAL INFILE '" + virtFile + "' INTO TABLE " + table
            + " FIELDS ENCLOSED BY '\\\''";
    LOGS(getLogger(), LOG_LVL_TRACE, "Load query: " << sql);
    return sql;
}

inline bool needClause(InsertColumnVector const& icv) {
    for(InsertColumnVector::const_iterator i=icv.begin(), e=icv.end();
        i != e; ++i) {
        if (!i->hexColumn.empty()) {
            return true;
        }
    }
    return false;
}
inline std::ostream& addSingleQuoted(std::ostream& os, std::string const& s) {
    return os << "'" << s << "'";
}
std::string formLoadInfile(std::string const& table,
                           std::string const& virtFile,
                           InsertColumnVector const& icv) {

    // Output should look something like this:
    // "LOAD DATA INFILE 'path.txt'
    // INTO TABLE mytable (column1, column2, @hexColumn3)
    // SET column3=UNHEX(@hexColumn3);"

    // Check icv to see if we need to hex/unhex
    if (!needClause(icv)) {
        return formLoadInfile(table, virtFile); // Use simpler version
    }
    std::ostringstream os;
    os << formLoadInfile(table, virtFile) << " (";
    // Input column list
    InsertColumnVector setColumns;
    InsertColumnVector::const_iterator i, b, e;
    for(i=b=icv.begin(), e=icv.end(); i != e; ++i) {
        if (i != b) {
            os << ",";
        }
        if (!i->hexColumn.empty()) {
            setColumns.push_back(*i);
            os << "@" << i->hexColumn;
        } else {
            addSingleQuoted(os, i->column);
        }
    }
    os << ") ";
    // Fixup SET statements
    for(i=b=setColumns.begin(), e=setColumns.end(); i != e; ++i) {
        if (i != b) {
            os << ", ";
        }
        os << "SET ";
        addSingleQuoted(os, i->column);
        os << "=UNHEX(" << "@" << i->hexColumn << ")";
    }
    return os.str();
}

}}} // namespace lsst::qserv::sql
