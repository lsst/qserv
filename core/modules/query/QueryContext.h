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
#include "mysql/MySqlConfig.h"
#include "proto/ScanTableInfo.h"
#include "qana/QueryMapping.h"
#include "query/DbTablePair.h"
#include "query/FromList.h"
#include "query/ValueExpr.h"
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
                 mysql::MySqlConfig const& mysqlSchemaCfg)
        : css(cssPtr), defaultDb(defDb), mysqlSchemaConfig(mysqlSchemaCfg) {}
    typedef std::vector<std::shared_ptr<QsRestrictor> > RestrList;

    std::shared_ptr<css::CssAccess> css;  ///< interface to CSS
    std::string defaultDb; ///< User session db context
    std::string dominantDb; ///< "dominant" database for this query
    std::string userName{"default"}; ///< unused, but reserved.

    mysql::MySqlConfig const mysqlSchemaConfig; ///< Used to connect to a database with the schema.
    std::map<std::string, DbTableSet> columnToTablesMap;


    proto::ScanInfo scanInfo; // Tables scanned (for shared scans)

    // Add a TableRef to the list of tables used by this query.
    // This typically contains the TableRefs from the FROM list.
    bool addUsedTableRef(std::shared_ptr<query::TableRef> const& tableRef);

    // Get a TableRef from the list of tables used by this query that matches the pased in TableRef.
    std::shared_ptr<query::TableRef> getTableRefMatch(
            std::shared_ptr<query::TableRef const> const& tableRef);

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

    void collectTopLevelTableSchema(FromList& fromList);
    std::string columnToTablesMapToString() const;
    std::vector<std::string> getTableSchema(std::string const& dbName, std::string const& tableName);

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
    std::vector<std::shared_ptr<query::TableRef>> _usedTableRefs; ///< TableRefs from the FROM list
    std::vector<std::shared_ptr<query::ValueExpr>> _usedValueExprs; ///< ValueExprs from the SELECT list
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QUERYCONTEXT_H
