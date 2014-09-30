// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/** @file
  * @brief A plugin for removing duplicate rows introduced by the match-
  *        table partitioner.
  */
// No public interface (no MatchTablePlugin.h)

// System headers
#include <string>

// Third-party headers
#include "boost/make_shared.hpp"

// Local headers
#include "css/Facade.h"
#include "parser/SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes
#include "qana/QueryPlugin.h"
#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/FromList.h"
#include "query/Predicate.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

using lsst::qserv::query::BoolFactor;
using lsst::qserv::query::BoolTermFactor;
using lsst::qserv::query::ColumnRef;
using lsst::qserv::query::CompPredicate;
using lsst::qserv::query::NullPredicate;
using lsst::qserv::query::OrTerm;
using lsst::qserv::query::PassTerm;
using lsst::qserv::query::TableRef;
using lsst::qserv::query::ValueExpr;
using lsst::qserv::query::ValueFactor;
using lsst::qserv::query::WhereClause;

namespace lsst {
namespace qserv {
namespace qana {

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
    typedef boost::shared_ptr<MatchTablePlugin> Ptr;

    virtual ~MatchTablePlugin() {}

    virtual void prepare() {}
    virtual void applyLogical(query::SelectStmt& stmt,
                              query::QueryContext& ctx);
    virtual void applyPhysical(QueryPlugin::Plan& p,
                               query::QueryContext& ctx) {}
};

/// MatchTablePluginFactory creates MatchTablePlugin instances.
class MatchTablePluginFactory : public QueryPlugin::Factory {
public:
    typedef boost::shared_ptr<MatchTablePluginFactory> Ptr;

    MatchTablePluginFactory() {}
    virtual ~MatchTablePluginFactory() {}

    virtual std::string getName() const {
        return "MatchTable";
    }
    virtual QueryPlugin::Ptr newInstance() {
        return boost::make_shared<MatchTablePlugin>();
    }
};

// MatchTablePlugin registration

namespace {
    struct registerPlugin {
        registerPlugin() {
            QueryPlugin::registerClass(
                 boost::make_shared<MatchTablePluginFactory>());
        }
    };

    registerPlugin registerTablePlugin; // static registration
}

// MatchTablePlugin implementation

void MatchTablePlugin::applyLogical(query::SelectStmt& stmt,
                                    query::QueryContext& ctx)
{
    if (stmt.getFromList().isJoin()) {
        // Do nothing. Query analysis and transformation for match table
        // joins is handled by the more general TablePlugin.
        return;
    }
    TableRef& t = *(stmt.getFromList().getTableRefList()[0]);
    css::Facade& f = *ctx.cssFacade;
    if (!f.isMatchTable(t.getDb(), t.getTable())) {
        return;
    }
    css::MatchTableParams mt = f.getMatchTableParams(t.getDb(), t.getTable());
    // Build the IR for the for duplicate filtering logic. Note that when
    // creating column references, there is no need to qualify the column name
    // (as db.table.column or alias.column). This is because the query is
    // guaranteed to operate on a single table and no column name ambiguities
    // are possible.
    //
    // First, create IR nodes for "dirCol1 IS NULL".
    boost::shared_ptr<NullPredicate> nullPred =
        boost::make_shared<NullPredicate>();
    nullPred->hasNot = false;
    nullPred->value = ValueExpr::newSimple(ValueFactor::newColumnRefFactor(
        boost::make_shared<ColumnRef>("", "", mt.dirColName1)));
    // Then create IR nodes for "flagCol<>2".
    boost::shared_ptr<CompPredicate> compPred =
        boost::make_shared<CompPredicate>();
    compPred->left = ValueExpr::newSimple(ValueFactor::newColumnRefFactor(
        boost::make_shared<ColumnRef>("", "", mt.flagColName)));
    compPred->op = SqlSQL2TokenTypes::NOT_EQUALS_OP;
    compPred->right = ValueExpr::newSimple(ValueFactor::newConstFactor("2"));
    // Create BoolFactors for each Predicate node.
    boost::shared_ptr<BoolFactor> bf1 = boost::make_shared<BoolFactor>();
    bf1->_terms.push_back(nullPred);
    boost::shared_ptr<BoolFactor> bf2 = boost::make_shared<BoolFactor>();
    bf2->_terms.push_back(compPred);
    // OR together the BoolFactors created above and place
    // inside a BoolTermFactor.
    boost::shared_ptr<OrTerm> bfs = boost::make_shared<OrTerm>();
    bfs->_terms.push_back(bf1);
    bfs->_terms.push_back(bf2);
    boost::shared_ptr<BoolTermFactor> btf =
        boost::make_shared<BoolTermFactor>();
    btf->_term = bfs;
    // Create PassTerm objects for parentheses.
    // TODO: remove this after DM-737 is resolved.
    boost::shared_ptr<PassTerm> openParen = boost::make_shared<PassTerm>();
    openParen->_text = "(";
    boost::shared_ptr<PassTerm> closeParen = boost::make_shared<PassTerm>();
    closeParen->_text = ")";
    // Wrap everything up in a BoolFactor
    boost::shared_ptr<BoolFactor> filter = boost::make_shared<BoolFactor>();
    filter->_terms.push_back(openParen);
    filter->_terms.push_back(btf);
    filter->_terms.push_back(closeParen);
    // Finally, insert the above into the WHERE clause.
    if (stmt.hasWhereClause()) {
        stmt.getWhereClause().prependAndTerm(filter);
    } else {
        boost::shared_ptr<WhereClause> where =
            boost::make_shared<WhereClause>();
        where->prependAndTerm(filter);
        stmt.setWhereClause(where);
    }
}

}}} // namespace lsst::qserv::qana
