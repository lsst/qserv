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



}}} // namespace lsst::qserv::query
