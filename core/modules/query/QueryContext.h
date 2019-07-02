// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 LSST Corporation.
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
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_QUERYCONTEXT_H
#define LSST_QSERV_QUERY_QUERYCONTEXT_H


// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "css/CssAccess.h"
#include "proto/ScanTableInfo.h"
#include "qana/QueryMapping.h"
#include "query/FromList.h"
#include "query/ValueExpr.h"
#include "sql/SqlConfig.h"
#include "global/stringTypes.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ColumnRef;
    class QsRestrictor;
    class TableRef;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// QueryContext is a value container for query state related to analyzing,
/// rewriting, and generating queries. It is the primary mechanism for
/// QueryPlugin instances to share information. It contains the user context of
/// a query, but not the query itself.
///
/// TODO: Think about QueryMapping's home. It is used during query manipulation,
/// contains information derived during analysis, and is used to generate
/// materialized query text.
class QueryContext {
public:
    typedef std::shared_ptr<QueryContext> Ptr;

    QueryContext(std::string const& defDb, std::shared_ptr<css::CssAccess> const& cssPtr,
                 sql::SqlConfig const& sqlCfg)
        : css(cssPtr), defaultDb(defDb), sqlConfig(sqlCfg) {}
    typedef std::vector<std::shared_ptr<QsRestrictor> > RestrList;

    std::shared_ptr<css::CssAccess> css;  ///< interface to CSS
    std::string defaultDb; ///< User session db context
    std::string dominantDb; ///< "dominant" database for this query
    std::string userName{"default"}; ///< unused, but reserved.

    sql::SqlConfig const sqlConfig; ///< Used to connect to a database with the schema.

    proto::ScanInfo scanInfo; // Tables scanned (for shared scans)

    /**
     * @brief Add a TableRef to the list of tables used by this query.
     *
     * Typical use for a SELECT statement would populate this with the TableRefs from the FROM list.
     */
    bool addUsedTableRef(std::shared_ptr<query::TableRef> const& tableRef);

    /**
     * @brief Get a complete TableRef used by the query that matches the pased-in TableRef.
     *
     * The passed in TableRef may be a subset or an alias of the returned TableRef.
     *
     * This does not do any verification that the database or table actually exist in the database instance,
     * but they must have been added via addUsedTableRef, which would typically indicate that they at least
     * exist in the FROM list of the query being processed.
     */
    std::shared_ptr<query::TableRef> getTableRefMatch(
        std::shared_ptr<query::TableRef> const& tableRef) const;

    /**
     * @brief Get complete a TableRef from the list of tables used by this query that matches the pased-in
     *        ColumnRef.
     *
     * This will verify that the column exists in the table specified by the ColumnRef.
     *
     * The table and database as indicated by the ColumnRef may be a subset or an alias of the TableRef
     * in the returned ColumnRef.
     */
    std::shared_ptr<query::TableRef> getTableRefMatch(
        std::shared_ptr<query::ColumnRef> const& columnRef) const;

    /**
     * @brief Add a ValueExpr that is used in the SELECT list.
     */
    void addUsedValueExpr(std::shared_ptr<query::ValueExpr> const& valueExpr);

    /**
     * @brief Get a ValueExpr from the list of ValueExprs used in the SELECT list that matches a given
     *        ValueExpr.
     *
     * This checks for two kinds of "match":
     * 1. Subset: Where the passed-in valueExpr partially or completely matches a valueExpr in the list.
     *            For example, "col" is a subset of "db.table.col". So is "table.col", and "db.table.col".
     *            However, if the alias of the passed-in ValueExpr is populated and does not match the alias
     *            of a valueExpr in the list, those two may not be a subset regardless of db, table, and
     *            column values.
     * 2. Alaised by: If the type of both ValueExprs is ColumnRef, then the 'table' value of the passed-in
     *                valueExpr might contain the alias name. E.g. for a user query
     *                "SELECT foo.col from tbl.col AS foo", "foo.col" in the SELECT list uses an alias to
     *                describe the same table as "tbl.col AS foo" in the FROM list, and the normalized
     *                ValueExpr is "db.tbl.col AS foo", so "foo.col" will be said to be 'aliased by'
     *                "db.tbl.col AS foo".
     *
     * @param valExpr the expr to match
     * @return std::shared_ptr<query::ValueExpr> that matching expr from the SELECT list, or nullptr
     */
    std::shared_ptr<query::ValueExpr> getValueExprMatch(
            std::shared_ptr<query::ValueExpr> const& valExpr) const;

    // Owned QueryMapping and query restrictors
    std::shared_ptr<qana::QueryMapping> queryMapping;
    std::shared_ptr<RestrList> restrictors;

    /**
     * @brief Get and cache database schema information for all the tables in the passed-in FROM list.
     */
    void collectTopLevelTableSchema(FromList& fromList);

    /**
     * @brief Get and cache database schema information for all the tables in the passed-in TableRef.
     *
     * Will include any joined TableRefs.
     */
    void collectTopLevelTableSchema(std::shared_ptr<query::TableRef> const& tableRef);

    std::string columnToTablesMapToString() const;

    int chunkCount{0}; //< -1: all, 0: none, N: #chunks

    bool needsMerge{false}; ///< Does this query require a merge/post-processing step?

    css::StripingParams getDbStriping() {
        return css->getDbStriping(dominantDb); }
    bool containsDb(std::string const& dbName) {
        return css->containsDb(dbName); }
    bool containsTable(std::string const& dbName, std::string const& tableName) {
        return css->containsTable(dbName, tableName); }
    bool hasChunks() const {
        return queryMapping.get() && queryMapping->hasChunks(); }
    bool hasSubChunks() const {
        return queryMapping.get() && queryMapping->hasSubChunks(); }

private:

    std::vector<std::string> _getTableSchema(std::string const& dbName, std::string const& tableName);

    // Comparison function for _columnToTablesMap to make the key string compare case insensitive.
    struct ColumnToTableLessThan {
        bool operator()(std::string const& lhs, std::string const& rhs) const;
    };

    // Comparison function for the TableRefSet that goes into the _columnToTablesMap, to compare the TableRef
    // objects, not the pointers that own them.
    struct TableRefSetLessThan {
        bool operator()(std::shared_ptr<query::TableRef const> const& lhs,
                        std::shared_ptr<query::TableRef const> const& rhs) const;
    };

    typedef std::set<std::shared_ptr<query::TableRef>, TableRefSetLessThan> TableRefSet;

    // stores the names of columns that are in each table that is used in the FROM statement.
    std::map<std::string, TableRefSet, ColumnToTableLessThan> _columnToTablesMap;

    std::vector<std::shared_ptr<query::TableRef>> _usedTableRefs; ///< TableRefs from the FROM list
    std::vector<std::shared_ptr<query::ValueExpr>> _usedValueExprs; ///< ValueExprs from the SELECT list
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QUERYCONTEXT_H
