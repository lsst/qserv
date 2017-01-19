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
#include "query/ColumnRef.h"
#include "sql/SqlConnection.h"
#include "sql/SqlResults.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.query.QueryContext");
}

namespace lsst {
namespace qserv {
namespace query {


/// Get the table schema for the tables mentioned in the SQL 'FROM' statement.
/// This should be adequate and possibly desirable as this information is being used
/// to restrict queries to particular nodes via the secondary index. Sub-queries are not
/// supported and even if they were, it could be difficult to determine if a restriction
/// in a sub-query would be a valid restriction on the entire query.
void QueryContext::collectTopLevelTableSchema(FromList& fromList) {
    columnToTablesMap.clear();
    for (TableRef::Ptr tblRefPtr : fromList.getTableRefList()) {
        DbTablePair dbTblPair(tblRefPtr->getDb(), tblRefPtr->getTable()); // DbTablePair has comparisons defined.
        if (dbTblPair.db.empty()) dbTblPair.db = defaultDb;
        LOGS(_log, LOG_LVL_DEBUG, "db=" << dbTblPair.db << " table=" << dbTblPair.table);
        if (!dbTblPair.db.empty() && !dbTblPair.table.empty()) {
            // Get the columns in the table from the DB schema and put them in the tableColumnMap.
            auto columns = getTableSchema(dbTblPair.db, dbTblPair.table);
            if (!columns.empty()) {
                for (auto const& col : columns) {
                    DbTableSet& st = columnToTablesMap[col];
                    st.insert(dbTblPair);
                }
            }
        }
    }
}


std::string QueryContext::columnToTablesMapToString() const {
    std::string str;
    for (auto const& elem : columnToTablesMap) {
        str += elem.first + "( "; // column name
        DbTableSet const& dbTblSet = elem.second;
        for (auto const& dbTbl : dbTblSet) {
            str += dbTbl.db + "." + dbTbl.table + " ";
        }
        str += ") ";
    }
    return str;
}


///
/// Get the table schema from the mysqlSchemaConfig database. Primarily, this is
/// used to map column names to particular tables.
std::vector<std::string> QueryContext::getTableSchema(std::string const& dbName,
                                                      std::string const& tableName) {
    // Get the table schema from the local database.
    DbTablePair dTPair(dbName, tableName);
    std::vector<std::string> colNames;
    sql::SqlConnection sqlConn{mysqlSchemaConfig};

    // SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    // WHERE table_name = 'tbl_name' AND table_schema = 'db_name'
    std::string sql("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ");
    sql += "WHERE table_name = '" + tableName + "' " +
           "AND table_schema = '" + dbName + "'";
    sql::SqlResults results;
    sql::SqlErrorObject errObj;
    if (not sqlConn.runQuery(sql, results, errObj)) {
        LOGS(_log, LOG_LVL_WARN, "getTableSchema query failed: " << sql);
        return colNames;
    }

    int j = 0;
    for (auto const& row : results) {
        colNames.emplace_back(row[0].first, row[0].second);
        ++j;
    }
    return colNames;
}


/// Resolve a column ref to a set of concrete (db,table).
/// In most cases this returns a set with a single element. It is possible that a non-fully qualified
/// column could exist in more than one table.
/// @return a set of concrete (db,table), based on current context.
DbTableSet QueryContext::resolve(std::shared_ptr<ColumnRef> cr) {
    DbTableSet dbTableSet;
    if (!cr) { return dbTableSet; }

    // If alias, retrieve real reference.
    if (cr->db.empty() && !cr->table.empty()) {
        DbTablePair concrete = tableAliases.get(cr->table);
        if (!concrete.empty()) {
            if (concrete.db.empty()) {
                concrete.db = defaultDb;
            }
            dbTableSet.insert(concrete);
            return dbTableSet;
        }
    }
    // Set default db and table.
    DbTablePair p;
    if (cr->table.empty()) { // No db or table.
        if (columnToTablesMap.empty()) {
            // This should only be the case if all of the table names in the from statement were invalid
            // or no connection to the database could be made (in which case there would be other errors
            // during normal operation).
            p = resolverTables[0];
            LOGS(_log, LOG_LVL_WARN,
                 "columnToTablesMap was empty, using first resolverTable. " << p.db << "." << p.table);
        } else {
            // Check the column name against the schema for the entries
            // on the columnToTablesMap, and choose the matching entry.
            // It is possible that the column exists in more than one table.
            if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
                LOGS(_log, LOG_LVL_DEBUG, "columnToTablesMap=" << columnToTablesMapToString());
            }
            std::string column = cr->column;
            if (!column.empty()) {
                auto iter = columnToTablesMap.find(column);
                if (iter != columnToTablesMap.end()) {
                    auto const& dTSet = iter->second;
                    for (auto const& dT : dTSet) {
                        dbTableSet.insert(dT);
                        LOGS(_log, LOG_LVL_DEBUG, "cr->table.empty column=" << column
                             << " adding " << dT.db << "." << dT.table);
                    }
                }
            }
            return dbTableSet;
        }
    } else if (cr->db.empty()) { // Table, but not alias.
        // Match against resolver stack
        DbTableVector::const_iterator i=resolverTables.begin(), e=resolverTables.end();
        for(; i != e; ++i) {
            if (i->table == cr->table) {
                p = *i;
                break;
            }
        }
        LOGS(_log, LOG_LVL_TRACE, "resolve found=" << (i != e) << " db.empty p=" << p.db << "." << p.table);
        if (i == e) return dbTableSet; // No resolution.
    } else { // both table and db exist, so return them
        DbTablePair dtp(cr->db, cr->table);
        dbTableSet.insert(dtp);
        return dbTableSet;
    }
    if (p.db.empty()) {
        // Fill partially-resolved empty db with user db context
        p.db = defaultDb;
    }
    LOGS(_log, LOG_LVL_TRACE, "resolve p=" << p.db << "." << p.table);
    {
        DbTablePair dtp(p.db, p.table);
        dbTableSet.insert(dtp);
    }
    return dbTableSet;
}


}}} // namespace lsst::qserv::query
