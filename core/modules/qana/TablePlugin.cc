/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @file TablePlugin.cc
  *
  * @brief TablePlugin implementation. TablePlugin replaces user query
  * table names with substitutable names and maintains a list of
  * tables that need to be substituted.
  *
  * @author Daniel L. Wang, SLAC
  */
// No public interface (no TablePlugin.h)

#include "qana/QueryPlugin.h"
#include <string>

#include "util/common.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/FromList.h"
#include "query/WhereClause.h"
#include "query/FuncExpr.h"
#include "qana/SphericalBoxStrategy.h"
#include "qana/TableStrategy.h"
#include "qana/QueryMapping.h"
#include "query/QueryContext.h"
#include "query/TableAlias.h"
#include "query/ValueFactor.h"
#include "log/Logger.h"

using lsst::qserv::query::DbTableVector;
namespace lsst {
namespace qserv {
namespace qana {

typedef std::list<std::string> StringList;


class addMap {
public:
    addMap(query::TableAlias& t, query::TableAliasReverse& r)
        : _tableAlias(t), _tableAliasReverse(r) {}
    void operator()(std::string const& alias,
                    std::string const& db, std::string const& table) {
        // LOGGER_INF << "set: " << alias << "->"
        //           << db << "." << table << std::endl;
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
        if(t.get()) { t->apply(*this); }
    }
    void operator()(query::TableRef& t) {
        std::string table = t.getTable();
        if(table.empty()) { throw std::logic_error("No table in TableRef"); }
        if(t.getDb().empty()) { t.setDb(context.defaultDb); }
        if(firstDb.empty()) { firstDb = t.getDb(); }
        if(firstTable.empty()) { firstTable = table; }
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
        if(t.get()) { t->apply(*this); }
    }
    void operator()(query::TableRef& t) {
        // LOGGER_INF << "tableref:";
        // t->putStream(LOG_STRM(Info));
        // LOGGER_INF << std::endl;
        // If no alias, then add one.
        std::string alias = t.getAlias();
        if(alias.empty()) {
            alias = _generate();
            t.setAlias(alias);
        }
        // Save ref
        _addMap(alias, t.getDb(), t.getTable());
    }
private:
    G _generate; // Functor that creates a new alias name
    A _addMap; // Functor that adds a new alias mapping for matchin
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
    explicit fixExprAlias(query::TableAliasReverse& r) : _tableAliasReverse(r) {}
    void operator()(query::ValueExprPtr& vep) {
        if(!vep.get()) {
            return;
        }
        // For each factor in the expr, patch for aliasing:
        query::ValueExpr::FactorOpList& factorOps = vep->getFactorOps();
        for(query::ValueExpr::FactorOpList::iterator i=factorOps.begin();
            i != factorOps.end(); ++i) {
            if(!i->factor) {
                throw std::logic_error("Bad ValueExpr::FactorOps");
            }
            query::ValueFactor& t = *i->factor;
            //LOGGER_INF << "fixing factor: " << *vep << std::endl;
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
                LOGGER_WRN << "Unhandled ValueFactor:" << t << std::endl;
                break;
            }
        }
    }
private:
    inline void _patchColumnRef(query::ColumnRef& ref) {
        std::string newAlias = _getAlias(ref.db, ref.table);
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.

        // Eliminate db. Replace table with aliased table.
        ref.db.assign("");
        ref.table.assign(newAlias);
    }

    inline void _patchFuncExpr(query::FuncExpr& fe) {
        std::for_each(fe.params.begin(), fe.params.end(),
                      fixExprAlias(_tableAliasReverse));
    }

    inline void _patchStar(query::ValueFactor& vt) {
        // TODO: No support for <db>.<table>.* in framework
        // Only <table>.* is supported.
        std::string newAlias = _getAlias("", vt.getTableStar());
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        else { vt.setTableStar(newAlias); }
    }

    inline std::string _getAlias(std::string const& db,
                                 std::string const& table) {
        //LOGGER_INF << "lookup: " << db << "." << table << std::endl;
        return _tableAliasReverse.get(db, table);
    }

    query::TableAliasReverse& _tableAliasReverse;
};

////////////////////////////////////////////////////////////////////////
// TablePlugin declaration
////////////////////////////////////////////////////////////////////////
/// TablePlugin is a query plugin that inserts placeholders for table
/// name substitution.
class TablePlugin : public QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<TablePlugin> Ptr;

    virtual ~TablePlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(query::SelectStmt& stmt, 
                              query::QueryContext& context);
    virtual void applyPhysical(QueryPlugin::Plan& p, 
                               query::QueryContext& context);
private:
    int _rewriteTables(qana::SelectStmtList& outList,
                       query::SelectStmt& in,
                       query::QueryContext& context,
                       boost::shared_ptr<qana::QueryMapping>& mapping);

    std::string _dominantDb;
};

////////////////////////////////////////////////////////////////////////
// TablePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class TablePluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<TablePluginFactory> Ptr;
    TablePluginFactory() {}
    virtual ~TablePluginFactory() {}

    virtual std::string getName() const { return "Table"; }
    virtual QueryPlugin::Ptr newInstance() {
        return QueryPlugin::Ptr(new TablePlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registerTablePlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
    TablePluginFactory::Ptr f(new TablePluginFactory());
    QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerTablePlugin;
} // annonymous namespace

////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
TablePlugin::applyLogical(query::SelectStmt& stmt, 
                          query::QueryContext& context) {
    // Idea: Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    query::FromList& fList = stmt.getFromList();
    // std::cout << "TABLE:Logical:orig fromlist "
    //           << fList.getGenerated()
    //           << (fList.isJoin() ? " is join" : "")
    //           << std::endl;

    query::TableRefList& tList = fList.getTableRefList();

    // For each tableref, modify to add alias.
    int seq=0;
    addMap addMapContext(context.tableAliases, context.tableAliasReverses);

    std::for_each(tList.begin(), tList.end(),
                  addAlias<generateAlias,addMap>(generateAlias(seq),
                                                 addMapContext));

    // Now snoop around the other clauses (SELECT, WHERE, etc. and
    // patch their table references)
    // select list
    query::SelectList& sList = stmt.getSelectList();
    query::ValueExprList& exprList = *sList.getValueExprList();
    std::for_each(exprList.begin(), exprList.end(),
                  fixExprAlias(context.tableAliasReverses));
    // where
    if(stmt.hasWhereClause()) {
        query::WhereClause& wClause = stmt.getWhereClause();
        query::WhereClause::ValueExprIter veI = wClause.vBegin();
        query::WhereClause::ValueExprIter veEnd = wClause.vEnd();
        std::for_each(veI, veEnd, fixExprAlias(context.tableAliasReverses));
    }
    // Fill-in default db context.
    DbTableVector v = fList.computeResolverTables();
    context.resolverTables.swap(v);

    query::DbTablePair p;
    addDbContext adc(context, p.db, p.table);
    std::for_each(tList.begin(), tList.end(), adc);
    _dominantDb = context.dominantDb = p.db;
    context.anonymousTable = p.table;

    // Apply function using the iterator...
    // wClause.walk(fixExprAlias(reverseAlias));
    // order by
    // having
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p, 
                           query::QueryContext& context) {
    // For each entry in original's SelectList, modify the SelectList
    // for the parallel and merge versions.
    // Set hasMerge to true if aggregation is detected.
    query::SelectList& oList = p.stmtOriginal.getSelectList();
    boost::shared_ptr<query::ValueExprList> vlist;
    vlist = oList.getValueExprList();
    if(!vlist) {
        throw std::logic_error("Invalid stmtOriginal.SelectList");
    }
    p.dominantDb = _dominantDb;

    typedef qana::SelectStmtList::iterator Iter;
    qana::SelectStmtList newList;
    for(Iter i=p.stmtParallel.begin(), e=p.stmtParallel.end();
        i != e; ++i) {
        int added;
        added = _rewriteTables(newList, **i, context, p.queryMapping);
        if(added == 0) {
            newList.push_back(*i);
        }
    }
    p.stmtParallel.swap(newList);
}

/// Patch the FromList tables in an input SelectStmt.
/// Or, if a query split is involved (to operate using overlap
/// tables), place new SelectStmts in the outList instead of patching
/// the existing SelectStmt.
/// This allows the caller to forgo excess SelectStmt manipulation by
/// reusing the existing SelectStmt in the common case where overlap
/// tables are not needed.
/// @return the number of statements added to the outList.
int TablePlugin::_rewriteTables(qana::SelectStmtList& outList,
                                query::SelectStmt& in,
                                query::QueryContext& context,
                                boost::shared_ptr<qana::QueryMapping>& mapping) {
    int added = 0;
    // Idea: Rewrite table names in from-list of the parallel
    // query. This is sufficient because table aliases were added in
    // the logical plugin stage so that real table refs should only
    // exist in the from-list.
    query::FromList& fList = in.getFromList();
    //    LOGGER_INF << "orig fromlist " << fList.getGenerated() << std::endl;

    // TODO: Better join handling by leveraging JOIN...ON syntax.
    // Before rewriting, compute the need for chunking and subchunking
    // based entirely on the FROM list. Queries that involve chunked
    // tables are necessarily chunked. Subchunking is inferred when
    // two chunked tables are joined (often the same table) and not on
    // a common key (key-equi-join). This check yields the decision:
    // ** for each table:
    //   availability of chunking and overlap
    //   desired chunking-level, with/without overlap
    // The QueryMapping abstraction provides a symbolic mapping so
    // that a later query generation stage can generate queries from
    // templatable queries a list of partition tuples.

    // In order for this to work while preserving join syntax, we
    // probably need to change the model. Previously, we did:
    // 1. Ingest a flattened sequence of tables.
    // 2. Look them up.
    // 3. (decide on subchunking)
    // 4. Create the new FromList entirely from the sequence.
    // We can ingest in a way that allows step 4 to create not from
    // scratch, but by doing a filter-copy of the original FromList,
    // and replacing each table ref one at a time.  This preserves the
    // structure. It might be desirable to alter the structure as an
    // optimization, but this can come later.
#if 1
    TableStrategy ts(fList, context);
    int permutationCount = ts.getPermutationCount();
    if(permutationCount > 1)
        for(int i=0; i < permutationCount; ++i) {
            boost::shared_ptr<query::SelectStmt> stmt = in.clone();
            query::TableRefListPtr trl = 
                ts.getPermutation(i, fList.getTableRefList());
            query::FromList::Ptr f(new query::FromList(trl));
            stmt->setFromList(f);
            outList.push_back(stmt);
            ++added;
    } else {
        ts.setToPermutation(0, fList.getTableRefList());
    }
    qana::QueryMapping::Ptr qm = ts.exportMapping();
#else
    SphericalBoxStrategy s(fList, context);
    qana::QueryMapping::Ptr qm = s.getMapping();
    // Compute the new fromLists, and if there are more than one, then
    // clone the selectstmt for each copy.
    if(ts.needsMultiple()) {
        typedef std::list<boost::shared_ptr<query::FromList> > FromListList;
        typedef FromListList::iterator Iter;
        FromListList newFroms = s.computeNewFromLists();
        for(Iter i=newFroms.begin(), e=newFroms.end(); i != e; ++i) {
            boost::shared_ptr<query::SelectStmt> stmt = in.clone();
            stmt->setFromList(*i);
            outList.push_back(stmt);
            ++added;
        }
    } else {
        s.patchFromList(fList);
        // LOGGER_INF << "post-patched fromlist " << fList.getGenerated() << std::endl;
    }
#endif
    // Now add/merge the mapping to the Plan
    if(!mapping.get()) {
        mapping = qm;
    } else {
        mapping->update(*qm);
    }
    // Query generation needs to be sensitive to this.
    // If no subchunks are needed,

    //
    // For each tableref, modify to replace tablename with
    // substitutable.
    return added;
}

    bool testIfSecondary(query::BoolTerm& t) {
    // FIXME: Look for secondary key in the bool term.
    LOGGER_INF << "Testing ";
    t.putStream(LOG_STRM(Info)) << std::endl;
    return false;
}

}}} // namespace lsst::qserv::qana
