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
#include "mysql/SchemaFactory.h"
#include "query/ColumnRef.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.query.QueryContext");
}

namespace lsst {
namespace qserv {
namespace query {


void QueryContext::collectTopLevelTableSchema(FromList& fromList) {
    using ColumnSet=std::set<std::string>;
    std::map<DbTablePair, ColumnSet> tableColumnMap;
    for (TableRef::Ptr tblRefPtr : fromList.getTableRefList()) {
        DbTablePair dbTblPair(tblRefPtr->getDb(), tblRefPtr->getTable()); // DbTablePair has comparisons defined.
        if (!dbTblPair.db.empty() && !dbTblPair.table.empty()) {
            // Get the columns in the table from the DB schema and put them in the tableColumnMap.
            auto columns = getTableSchema(dbTblPair.db, dbTblPair.table);
            if (!columns.empty()) {
                ColumnSet& colSet = tableColumnMap[dbTblPair];
                for (auto const& col : columns) {
                    colSet.insert(col);
                }
            }
        }
    }

    // Use the tableToColumnMap to create the columnToTablesMap.
    columnToTablesMap.clear();
    for (auto const& tblToColPair : tableColumnMap) {
        auto const& dbTbl = tblToColPair.first;
        auto const& colSet = tblToColPair.second;
        for (auto const& col : colSet) {
            DbTableSet& st = columnToTablesMap[col];
            st.insert(dbTbl);
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


std::vector<std::string> QueryContext::getTableSchema(std::string const& dbName, std::string const& tableName) {
    // Check if it is in our list of table schemas
    DbTablePair dTPair(dbName, tableName);

    // Get the table schema from the local database.
    std::vector<std::string> colNames;
    mysql::MySqlConnection mysqlConn{mysqlSchemaConfig};
    if (!mysqlConn.connected()) {
        LOGS(_log, LOG_LVL_ERROR, "QueryContext::getTableSchema failed to connect to schema database!");
        return colNames; // Reconnection failed. This is an error.
    }

    // SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'tbl_name' AND table_schema = 'db_name'
    std::string sql("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ");
    sql += "WHERE table_name = '" + tableName + "' " +
           "AND table_schema = '" + dbName + "'";
    LOGS(_log, LOG_LVL_DEBUG, "tableSchema sql=" << sql);
    int rc = mysql_real_query(mysqlConn.getMySql(), sql.data(), sql.size());

    if (rc) { return colNames; }
    MYSQL_RES* result = mysql_use_result(mysqlConn.getMySql());

    auto resSchema = mysql::SchemaFactory::newFromResult(result);
    std::string resSchemaStr;
    for(auto i=resSchema.columns.begin(), e=resSchema.columns.end(); i != e; ++i) {
        resSchemaStr += i->name + ", ";
    }
    LOGS(_log, LOG_LVL_DEBUG, "tableSchema resSchema=" << resSchemaStr);

    // int numFields = mysql_num_fields(result); &&&
    MYSQL_ROW row;
    int j = 0;
    while ((row = mysql_fetch_row(result))) {
        auto lengths = mysql_fetch_lengths(result);
        colNames.emplace_back(row[0], lengths[0]);
        LOGS(_log, LOG_LVL_DEBUG, "tableSchema row[" << j << "]=" << colNames[j]);
        ++j;
    }
    return colNames;
}


/// Resolve a column ref to a concrete (db,table)
/// @return the concrete (db,table), based on current context.
DbTableSet QueryContext::resolve(std::shared_ptr<ColumnRef> cr) {   //&&& needs to return list of DbTablePair or QservRestrictorPlugin lookupSecIndex() and getSecIndexRestrictors need work
    LOGS(_log, LOG_LVL_DEBUG, "&&& resolve cr=" << cr);
    DbTableSet dbTableSet;
    if (!cr) { return dbTableSet; }

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
            dbTableSet.insert(concrete);
            return dbTableSet;
        }
    }
    // Set default db and table.
    DbTablePair p;
    if (cr->table.empty()) { // No db or table: choose first resolver pair
        // p = resolverTables[0]; &&&
        // LOGS(_log, LOG_LVL_DEBUG, "&&& resolve table.empty p=" << dbTableStr(p)); &&&
        // TODO: We can be fancy and check the column name against the
        // schema for the entries on the resolverTables, and choose
        // the matching entry.
        std::string column = cr->column;
        std::string debugMsg; // &&& keep or not?
        if (!column.empty()) {
            auto iter = columnToTablesMap.find(column);
            if (iter != columnToTablesMap.end()) {
                auto const& dTSet = iter->second;
                for (auto const& dT : dTSet) {
                    dbTableSet.insert(dT);
                    debugMsg += dT.db + "." + dT.table + ",";
                }
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve table.empty " << debugMsg);
        return dbTableSet;
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
        if (i == e) return dbTableSet; // No resolution.
    } else { // both table and db exist, so return them
        DbTablePair dtp(cr->db, cr->table);
        dbTableSet.insert(dtp);
        return dbTableSet;
    }
    if (p.db.empty()) {
        // Fill partially-resolved empty db with user db context
        LOGS(_log, LOG_LVL_DEBUG, "&&& resolve p.db.empty");
        p.db = defaultDb;
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& resolve p=" << dbTableStr(p));
    {
        DbTablePair dtp(p.db, p.table);
        dbTableSet.insert(dtp);
    }
    return dbTableSet;
}


}}} // namespace lsst::qserv::query
