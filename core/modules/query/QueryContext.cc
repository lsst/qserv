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
#include "sql/SqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
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


std::shared_ptr<query::TableRef> QueryContext::getTableRefMatch(
        std::shared_ptr<query::TableRef> const& tableRef) const {
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


std::shared_ptr<query::TableRef>
QueryContext::getTableRefMatch(std::shared_ptr<query::ColumnRef> const& columnRef) const {
    auto mapItr = _columnToTablesMap.find(columnRef->getColumn());
    if (_columnToTablesMap.end() == mapItr)
        return nullptr;
    for (auto tableRef : mapItr->second) {
        auto&& tableRefMatch = getTableRefMatch(tableRef);
        if (tableRefMatch != nullptr) {
            tableRef = tableRefMatch;
        }
        if (columnRef->getTableRef()->isSubsetOf(*tableRef)) {
            return tableRef;
        } else if (columnRef->getTableRef()->isAliasedBy(*tableRef)) {
            return tableRef;
        }
    }
    return nullptr;
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

    std::string db = tableRef->getDb();
    if (db.empty()) db = defaultDb;
    std::string table = tableRef->getTable();
    LOGS(_log, LOG_LVL_DEBUG, "db=" << db << " table=" << table);
    if (not db.empty() && not table.empty()) {
        // Get the columns in the table from the DB schema and put them in the tableColumnMap.
        auto columns = _getTableSchema(db, table);
        if (!columns.empty()) {
            for (auto const& col : columns) {
                // note that we don't copy the join into the new table ref; keep the new TableRef "simple".
                auto addTableRef = std::make_shared<query::TableRef>(db, table, tableRef->getAlias());
                LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << "adding " << *addTableRef << " for column:" << col);
                auto& tableRefSet = _columnToTablesMap[col];
                tableRefSet.insert(addTableRef);
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
    std::ostringstream os;
    for (auto const& elem : _columnToTablesMap) {
        os << elem.first <<  "( "; // column name
        auto const& tableRefSet = elem.second;
        for (auto const& tableRef : tableRefSet) {
            os << *tableRef;
        }
        os << ") ";
    }
    return os.str();
}


/// Get the table schema from the mysqlSchemaConfig database. Primarily, this is
/// used to map column names to particular tables.
std::vector<std::string> QueryContext::_getTableSchema(std::string const& dbName,
                                                      std::string const& tableName) {
    std::vector<std::string> colNames;
    sql::SqlConfig cfg;
    cfg.mySqlConfig = mysqlSchemaConfig;
    auto sqlConn = sql::SqlConnectionFactory::make(cfg);
    sql::SqlErrorObject errObj;
    sqlConn->listColumns(colNames, errObj, dbName, tableName);
    return colNames;
}


bool QueryContext::ColumnToTableLessThan::operator()(std::string const& lhs, std::string const& rhs) const {
    std::string l(lhs);
    std::string r(rhs);
    std::transform(l.begin(), l.end(), l.begin(), [](unsigned char c){ return tolower(c); });
    std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){ return tolower(c); });
    return l < r;
}


bool QueryContext::TableRefSetLessThan::operator()(std::shared_ptr<query::TableRef const> const& lhs,
        std::shared_ptr<query::TableRef const> const& rhs) const {
    if (nullptr == lhs || nullptr == rhs) {
        throw std::runtime_error("nullptr in TableRefSetLessThan");
    }
    return *lhs < *rhs;
}


}}} // namespace lsst::qserv::query
