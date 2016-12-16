// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QUERY_QUERYCONTEXT_H
#define LSST_QSERV_QUERY_QUERYCONTEXT_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

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
#include "query/TableAlias.h"
#include "global/stringTypes.h"

namespace lsst {
namespace qserv {
namespace query {

class ColumnRef;
class QsRestrictor;

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
    std::string anonymousTable; ///< Implicit table context
    std::string userName{"default"}; ///< unused, but reserved.
    std::vector<DbTablePair> resolverTables; ///< Implicit column resolution context. Will obsolete anonymousTable.
    mysql::MySqlConfig const mysqlSchemaConfig; ///< Used to connect to a database with the schema.
    std::map<std::string, DbTableSet> columnToTablesMap;


    proto::ScanInfo scanInfo; // Tables scanned (for shared scans)

    // Table aliasing
    TableAlias tableAliases;
    TableAliasReverse tableAliasReverses;

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
    // DbTablePair resolve(std::shared_ptr<ColumnRef> cr); &&& delete
    DbTableSet resolve(std::shared_ptr<ColumnRef> cr);
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QUERYCONTEXT_H
