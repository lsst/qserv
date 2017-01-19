// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_TABLEPLUGIN_H
#define LSST_QSERV_QANA_TABLEPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"


namespace lsst {
namespace qserv {
namespace qana {


/// TablePlugin is a query plugin that inserts placeholders for table
/// name substitution.
class TablePlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<TablePlugin> Ptr;

    virtual ~TablePlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(query::SelectStmt& stmt,
                              query::QueryContext& context);
    virtual void applyPhysical(QueryPlugin::Plan& p,
                               query::QueryContext& context);
private:
    int _rewriteTables(SelectStmtPtrVector& outList,
                       query::SelectStmt& in,
                       query::QueryContext& context,
                       std::shared_ptr<qana::QueryMapping>& mapping);

    std::string _dominantDb;
};


/// MatchTablePlugin fixes up queries on match tables which are not joins
/// so that they do not return duplicate rows potentially introduced by
/// the partitioning process.
///
/// Recall that a match table provides a spatially constrained N-to-M mapping
/// between two director-tables via their primary keys. The partitioner
/// assigns a row from a match table to a chunk S whenever either matched
/// entity belongs to S. Therefore, if the two matched entities lie in
/// different chunks, a copy of the corresponding match will be stored in
/// two chunks. The partitioner also stores partitioning flags F for each
/// output row as follows:
///
/// - Bit 0 (the LSB of F), is set if the chunk of the first entity in the
///   match is equal to the chunk containing the row.
/// - Bit 1 is set if the chunk of the second entity is equal to the
///   chunk containing the row.
///
/// So, if rows with a non-null first-entity reference and partitioning flags
/// set to 2 are removed, then duplicates introduced by the partitioner will
/// not be returned.
///
/// This plugin's task is to recognize queries on match tables which are not
/// joins, and to add the filtering logic described above to their WHERE
/// clauses.
///
/// Determining whether a table is a match table or not requires a metadata
/// lookup. This in turn requires knowledge of that table's containing
/// database. As a result, MatchTablePlugin must run after TablePlugin.
class MatchTablePlugin : public QueryPlugin {
public:
    typedef std::shared_ptr<MatchTablePlugin> Ptr;

    virtual ~MatchTablePlugin() {}

    virtual void prepare() {}
    virtual void applyLogical(query::SelectStmt& stmt,
                              query::QueryContext& ctx);
    virtual void applyPhysical(QueryPlugin::Plan& p,
                               query::QueryContext& ctx) {}
};


}}} // namespace lsst::qserv::qana

#endif /* LSST_QSERV_QANA_TABLEPLUGIN_H */
