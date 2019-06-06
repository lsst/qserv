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
#include "query/JoinRef.h"
#include "sql/SqlConnection.h"
#include "sql/SqlResults.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.QueryContext");

}

namespace lsst {
namespace qserv {
namespace query {


bool QueryContext::addUsedTableRef(std::shared_ptr<query::TableRef> const& tableRef) {
    if (nullptr == tableRef) {
        return false;
    }
    // TableRefs added from the FROM list can have JoinRefs, which are nonsensical anywhere but in the FROM
    // list. To prevent these from leaking into other parts of the statement, copy the TableRef but omit the
    // JoinRefs.
    auto addTableRef = std::make_shared<query::TableRef>(
            tableRef->getDb(), tableRef->getTable(), tableRef->getAlias());
    for (auto const& usedTableRef : _usedTableRefs) {
        // If the TableRef is already represented in the list (fully & exactly - but without joins) then just
        // return true.
        if (*usedTableRef == *addTableRef) {
            return true;
        }
        // At a minimum, make sure we aren't accepting a second tableRef with different db or table but the same alias
        if (usedTableRef->getAlias() == addTableRef->getAlias()) {
            return false;
        }
    }
    _usedTableRefs.push_back(addTableRef);
    return true;
}


std::shared_ptr<query::TableRef> QueryContext::getTableRefMatch const(
        std::shared_ptr<query::TableRef const> const& tableRef) {
    if (nullptr == tableRef) {
        return nullptr;
    }
    // This should not be used with TableRefs that contain a join.
    if (not tableRef->isSimple()) {
        return nullptr;
    }
    for (auto&& usedTableRef : _usedTableRefs) {
        if (tableRef->isSubsetOf(*usedTableRef)) {
            return usedTableRef;
        }
        if (tableRef->isAliasedBy(*usedTableRef)) {
            return usedTableRef;
        }
    }
    return nullptr;
}


std::vector<std::shared_ptr<query::TableRef>>
QueryContext::getTableRefMatches(std::shared_ptr<query::ColumnRef const> const& columnRef) const {
    auto mapItr = _columnToTablesMap.find(columnRef->getColumn());
    if (_columnToTablesMap.end() == mapItr)
        return std::vector<std::shared_ptr<query::TableRef>>();

    std::vector<std::shared_ptr<query::TableRef>> tablesWithColumn;
    // make a TableRef for each DbTablePair in the map that matches the column, so we can test it/them.
    for (auto const& dbTablePair : mapItr->second) {
        tablesWithColumn.emplace_back(std::make_shared<query::TableRef>(dbTablePair.db, dbTablePair.table, ""));
    }

    std::vector<std::shared_ptr<query::TableRef>> retTableRefs;
    for (auto&& tableWithColumn : tablesWithColumn) {
        if (columnRef->getTableRef()->isSubsetOf(*tableWithColumn)) {
            retTableRefs.push_back(tableWithColumn);
        } else if (columnRef->getTableRef()->isAliasedBy(*tableWithColumn)) {
            retTableRefs.push_back(tableWithColumn);
        }
    }
    return retTableRefs;
}


void QueryContext::addUsedValueExpr(std::shared_ptr<query::ValueExpr> const& valueExpr) {
    _usedValueExprs.push_back(valueExpr);
}


std::shared_ptr<query::ValueExpr>
QueryContext::getValueExprMatch(std::shared_ptr<query::ValueExpr> const& valExpr) const {
    for (auto&& usedValExpr : _usedValueExprs) {
        if (valExpr->isSubsetOf(*usedValExpr)) {
            return usedValExpr;
        }
        if (valExpr->isColumnRef() && usedValExpr->isColumnRef()) {
            if (valExpr->getColumnRef()->isAliasedBy(*usedValExpr->getColumnRef())) {
                return usedValExpr;
            }
        }
    }
    return nullptr;
}



/// Get the table schema for the tables mentioned in the SQL 'FROM' statement.
/// This should be adequate and possibly desirable as this information is being used
/// to restrict queries to particular nodes via the secondary index. Sub-queries are not
/// supported and even if they were, it could be difficult to determine if a restriction
/// in a sub-query would be a valid restriction on the entire query.
void QueryContext::collectTopLevelTableSchema(FromList& fromList) {
    _columnToTablesMap.clear();
    for (TableRef::Ptr tableRef : fromList.getTableRefList()) {
        collectTopLevelTableSchema(tableRef);
    }
}


void QueryContext::collectTopLevelTableSchema(std::shared_ptr<query::TableRef> const& tableRef) {
    DbTablePair dbTblPair(tableRef->getDb(), tableRef->getTable()); // DbTablePair has comparisons defined.
    if (dbTblPair.db.empty()) dbTblPair.db = defaultDb;
    LOGS(_log, LOG_LVL_DEBUG, "db=" << dbTblPair.db << " table=" << dbTblPair.table);
    if (!dbTblPair.db.empty() && !dbTblPair.table.empty()) {
        // Get the columns in the table from the DB schema and put them in the tableColumnMap.
        auto columns = getTableSchema(dbTblPair.db, dbTblPair.table);
        if (!columns.empty()) {
            for (auto const& col : columns) {
                LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << "adding " << dbTblPair << " for column:" << col);
                DbTableSet& st = _columnToTablesMap[col];
                st.insert(dbTblPair);
            }
        }
    }
    for (auto const& joinRef : tableRef->getJoins()) {
        auto const& rightTableRef = joinRef->getRight();
        if (rightTableRef != nullptr) {
            collectTopLevelTableSchema(rightTableRef);
        }
    }
}


std::string QueryContext::columnToTablesMapToString() const {
    std::string str;
    for (auto const& elem : _columnToTablesMap) {
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


bool QueryContext::ColumnToTableLessThan::operator()(std::string const& lhs, std::string const& rhs) const {
    std::string l(lhs);
    std::string r(rhs);
    std::transform(l.begin(), l.end(), l.begin(), [](unsigned char c){ return tolower(c); });
    std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){ return tolower(c); });
    return l < r;
}

}}} // namespace lsst::qserv::query
