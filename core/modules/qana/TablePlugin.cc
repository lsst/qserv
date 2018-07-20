// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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

/// \file
/// \brief TablePlugin implementation.
///
/// TablePlugin modifies the parsed query to assign an alias to all the table
/// references in the query from-list. It then rewrites all column references
/// (e.g. in the where clause) to use the appropriate aliases. This allows
/// changing a table reference in a query without editing anything except the
/// from-clause.
///
/// During the concrete query planning phase, TablePlugin determines whether
/// each query proposed for parallel (worker-side) execution is actually
/// parallelizable and how this should be done - that is, it determines whether
/// or not sub-chunking should be used and which director table(s) to use
/// overlap for. Finally, it rewrites table references to use name patterns
/// into which (sub-)chunk numbers can be substituted. This act of substitution
/// is the final step in generating the queries sent out to workers.
///
/// \author Daniel L. Wang, SLAC

// Class header
#include "qana/TablePlugin.h"

// System headers
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/QueryMapping.h"
#include "qana/RelationGraph.h"
#include "qana/TableInfoPool.h"

#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableAlias.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/common.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.TablePlugin");
}

namespace lsst {
namespace qserv {
namespace qana {

class addMap {
public:
    addMap(query::TableAlias& t, query::TableAliasReverse& r)
        : _tableAlias(t), _tableAliasReverse(r) {}
    void operator()(std::string const& alias,
                    std::string const& db, std::string const& table) {
        _tableAlias.set(db, table, alias);
        _tableAliasReverse.set(db, table, alias);
    }

    query::TableAlias& _tableAlias;
    query::TableAliasReverse& _tableAliasReverse;
};

class generateAlias {
public:
    explicit generateAlias(int& seqN) : _seqN(seqN) {}
    std::string operator()() {
        std::stringstream ss;
        ss << "QST_" << ++_seqN << "_";
        return ss.str();
    }
    int& _seqN;
};

class addDbContext : public query::TableRef::Func {
public:
    addDbContext(query::QueryContext const& c,
                 std::string& firstDb_,
                 std::string& firstTable_)
        : context(c), firstDb(firstDb_), firstTable(firstTable_)
        {}
    void operator()(query::TableRef::Ptr t) {
        if (t.get()) { t->apply(*this); }
    }
    void operator()(query::TableRef& t) {
        std::string table = t.getTable();
        if (table.empty()) { throw std::logic_error("No table in TableRef"); }
        if (t.getDb().empty()) { t.setDb(context.defaultDb); }
        if (firstDb.empty()) { firstDb = t.getDb(); }
        if (firstTable.empty()) { firstTable = table; }
    }
    query::QueryContext const& context;
    std::string& firstDb;
    std::string& firstTable;
};

template <typename G, typename A>
class addAlias : public query::TableRef::Func {
public:
    addAlias(G g, A a) : _generate(g), _addMap(a) {}
    void operator()(query::TableRef::Ptr t) {
        if (t.get()) { t->apply(*this); }
    }
    void operator()(query::TableRef& t) {
        // If no alias, then add one.
        std::string alias = t.getAlias();
        if (alias.empty()) {
            alias = _generate();
            t.setAlias(alias);
        }
        // Save ref
        _addMap(alias, t.getDb(), t.getTable());
    }
private:
    G _generate; // Functor that creates a new alias name
    A _addMap; // Functor that adds a new alias mapping for matching
               // later clauses.
};

////////////////////////////////////////////////////////////////////////
// fixExprAlias is a functor that acts on ValueExpr objects and
// modifys them in-place, altering table names to use an aliased name
// that is mapped via TableAliasReverse.
// It does not add table qualifiers where none already exist, because
// there is no compelling reason to do so (yet).
////////////////////////////////////////////////////////////////////////
class fixExprAlias {
public:
    fixExprAlias(std::string const& db, query::TableAliasReverse& r) :
        _defaultDb(db), _tableAliasReverse(r) {}

    void operator()(query::ValueExprPtr& vep) {
        if (!vep.get()) {
            return;
        }
        // For each factor in the expr, patch for aliasing:
        query::ValueExpr::FactorOpVector& factorOps = vep->getFactorOps();
        for(query::ValueExpr::FactorOpVector::iterator i=factorOps.begin();
            i != factorOps.end(); ++i) {
            if (!i->factor) {
                throw std::logic_error("Bad ValueExpr::FactorOps");
            }
            query::ValueFactor& t = *i->factor;
            switch(t.getType()) {
            case query::ValueFactor::COLUMNREF:
                // check columnref.
                _patchColumnRef(*t.getColumnRef());
                break;
            case query::ValueFactor::FUNCTION:
            case query::ValueFactor::AGGFUNC:
                // recurse for func params (aggfunc is special case of function)
                _patchFuncExpr(*t.getFuncExpr());
                break;
            case query::ValueFactor::STAR:
                // Patch db/table name if applicable
                _patchStar(t);
                break;
            case query::ValueFactor::CONST:
                break; // Constants don't need patching.
            default:
                LOGS(_log, LOG_LVL_WARN, "Unhandled ValueFactor:" << t);
                break;
            }
        }
    }

private:
    void _patchColumnRef(query::ColumnRef& ref) {
        std::string newAlias = _getAlias(ref.db, ref.table);
        if (newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.

        // Eliminate db. Replace table with aliased table.
        ref.db.assign("");
        ref.table.assign(newAlias);
    }

    void _patchFuncExpr(query::FuncExpr& fe) {
        std::for_each(fe.params.begin(), fe.params.end(),
                      fixExprAlias(_defaultDb, _tableAliasReverse));
    }

    void _patchStar(query::ValueFactor& vt) {
        // TODO: No support for <db>.<table>.* in framework
        // Only <table>.* is supported.
        std::string newAlias = _getAlias("", vt.getConstVal());
        if (newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        else { vt.setConstVal(newAlias); }
    }

    std::string _getAlias(std::string const& db,
                          std::string const& table) {
        return _tableAliasReverse.get(db.empty() ? _defaultDb : db, table);
    }

    std::string const& _defaultDb;
    query::TableAliasReverse& _tableAliasReverse;
};

////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
TablePlugin::applyLogical(query::SelectStmt& stmt,
                          query::QueryContext& context) {

    query::FromList& fList = stmt.getFromList();
    context.collectTopLevelTableSchema(fList);
    query::TableRefList& tList = fList.getTableRefList();
    // Fill-in default db context.
    query::DbTableVector v = fList.computeResolverTables();
    context.resolverTables.swap(v);
    query::DbTablePair p;
    addDbContext adc(context, p.db, p.table);
    std::for_each(tList.begin(), tList.end(), adc);
    _dominantDb = context.dominantDb = p.db;
    context.anonymousTable = p.table;

    // Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    //
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    //
    // Note also that this must happen after the default db context
    // has been filled in, or alias lookups will be incorrect.

    // For each tableref, modify to add alias.
    int seq=0;
    addMap addMapContext(context.tableAliases, context.tableAliasReverses);
    std::for_each(tList.begin(), tList.end(),
                  addAlias<generateAlias,addMap>(generateAlias(seq), addMapContext));

    // Patch table references in the select list,
    query::SelectList& sList = stmt.getSelectList();
    query::ValueExprPtrVector& exprList = *sList.getValueExprList();
    std::for_each(exprList.begin(), exprList.end(), fixExprAlias(
        context.defaultDb, context.tableAliasReverses));
    // where clause,
    if (stmt.hasWhereClause()) {
        query::ValueExprPtrVector e;
        stmt.getWhereClause().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliasReverses));
    }
    // group by clause,
    if (stmt.hasGroupBy()) {
        query::ValueExprPtrVector e;
        stmt.getGroupBy().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliasReverses));
    }
    // having clause,
    if (stmt.hasHaving()) {
        query::ValueExprPtrVector e;
        stmt.getHaving().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliasReverses));
    }
    // order by clause,
    if (stmt.hasOrderBy()) {
        query::ValueExprPtrVector e;
        stmt.getOrderBy().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliasReverses));
    }
    // and in the on clauses of all join specifications.
    typedef query::TableRefList::iterator TableRefIter;
    typedef query::JoinRefPtrVector::iterator JoinRefIter;
    for (TableRefIter t = tList.begin(), te = tList.end(); t != te; ++t) {
        query::JoinRefPtrVector& joinRefs = (*t)->getJoins();
        for (JoinRefIter j = joinRefs.begin(), je = joinRefs.end(); j != je; ++j) {
            std::shared_ptr<query::JoinSpec> spec = (*j)->getSpec();
            if (spec) {
                fixExprAlias fix(context.defaultDb, context.tableAliasReverses);
                // A column name in a using clause should be unqualified,
                // so only patch on clauses.
                std::shared_ptr<query::BoolTerm> on = spec->getOn();
                if (on) {
                    query::ValueExprPtrVector e;
                    on->findValueExprs(e);
                    std::for_each(e.begin(), e.end(), fix);
                }
            }
        }
    }
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p,
                           query::QueryContext& context)
{
    TableInfoPool pool(context.defaultDb, *context.css);
    if (!context.queryMapping) {
        context.queryMapping = std::make_shared<QueryMapping>();
    }
    // Process each entry in the parallel select statement set.
    typedef SelectStmtPtrVector::iterator Iter;
    SelectStmtPtrVector newList;
    for(Iter i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
        RelationGraph g(**i, pool);
        g.rewrite(newList, *context.queryMapping);
    }
    p.dominantDb = _dominantDb;
    p.stmtParallel.swap(newList);
}

}}} // namespace lsst::qserv::qana
