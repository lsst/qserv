// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
/**
  * @file
  *
  * @brief QueryContext implementation.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/QueryContext.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConnection.h"
#include "query/ColumnRef.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.query.QueryContext");
}

namespace lsst {
namespace qserv {
namespace query {

/* // &&&
/// Below function from Evan Terran from stack overflow.
// trim from start (in place)
static inline void trimLeft(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(),
            std::not1(std::ptr_fun<int, int>(std::isspace))));
}

// trim from end (in place)
static inline void trimRight(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
            std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
    ltrim(s);
    rtrim(s);
}

std::string trimmed(std::string s) {

}



std::vector<std::string> splitString(std::string const& inStr, std::string const& delimiters) {
    std::vector<std::string> tokens;
    std::string tk;
    for (auto i = inStr.begin(), e = inStr.end(); i != e; i++) {
        auto c = *i;
        bool found = false;
        for (auto const& d : delimiters) {
            if (d == c) {
                found = true;
                break;
            }
        }
        if (!found) {
            tk += c;
        } else {
            tokens.push_back(tk);
            tk.clear();
        }
    }
    return tokens;
}

void QueryContext::getTableSchema(std::string const& dbName, std::string const& tableName) {
    auto schemaStr = css->getTableSchema(dbName, tableName);
    // That should get a string that looks something like
    // "`sourceId` bigint(20) NOT NULL,`scienceCcdExposureId` bigint(20) DEFAULT NULL,
    // `objectId` bigint(20) DEFAULT NULL,..."
    std:string comma = ",";
    auto colEntries = split(schemaStr, comma);

    using column = std::vector<std::string>;
    std::vector<column> columns;
    for (std::string& s : colEntries) {
        trim(s);

    }

}

*/ // &&&

void QueryContext::getTableSchema(std::string const& dbName, std::string const& tableName) {
    //mysql::MySqlConfig mysqlResultConfig{czarConfig.getMySqlResultConfig()}; &&&
    // &&& Get the table schema from the local database.
    mysql::MySqlConnection _mysqlConn{mysqlSchemaConfig};

}

/// Resolve a column ref to a concrete (db,table)
/// @return the concrete (db,table), based on current context.
DbTablePair
QueryContext::resolve(std::shared_ptr<ColumnRef> cr) {
    LOGS(_log, LOG_LVL_DEBUG, "&&& resolve cr=" << cr);
    if (!cr) { return DbTablePair(); }

    auto dbTableStr = [](DbTablePair const& dbT) -> std::string { // &&& delete
        std::string str = dbT.db + "." + dbT.table;
        return str;
    };

    // If alias, retrieve real reference.
    if (cr->db.empty() && !cr->table.empty()) {
        DbTablePair concrete = tableAliases.get(cr->table);
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve db.empty concrete=" << dbTableStr(concrete));
        if (!concrete.empty()) {
            if (concrete.db.empty()) {
                concrete.db = defaultDb;
            }
            return concrete;
        }
    }
    // Set default db and table.
    DbTablePair p;
    if (cr->table.empty()) { // No db or table: choose first resolver pair
        p = resolverTables[0];
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve table.empty p=" << dbTableStr(p));
        // TODO: We can be fancy and check the column name against the
        // schema for the entries on the resolverTables, and choose
        // the matching entry.
    } else if (cr->db.empty()) { // Table, but not alias.
        // Match against resolver stack
        DbTableVector::const_iterator i=resolverTables.begin(), e=resolverTables.end(); // &&& auto
        for(; i != e; ++i) {
            if (i->table == cr->table) {
                p = *i;
                break;
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve found=" << (i != e) << " db.empty p=" << dbTableStr(p));
        if (i == e) return DbTablePair(); // No resolution.
    } else { // both table and db exist, so return them
        return DbTablePair(cr->db, cr->table);
    }
    if (p.db.empty()) {
        // Fill partially-resolved empty db with user db context
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve p.db.empty");
        p.db = defaultDb;
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& resolve p=" << dbTableStr(p));
    return p;
}

}}} // namespace lsst::qserv::query
