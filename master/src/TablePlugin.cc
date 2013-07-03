/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#include <string>

#include "lsst/qserv/master/QueryPlugin.h"

#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/SphericalBoxStrategy.h"
#include "lsst/qserv/master/QueryMapping.h"
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/ValueFactor.h"

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
class ReverseAlias {
public:
    ReverseAlias() {}
    std::string const& get(std::string db, std::string table) {
        return _map[makeKey(db, table)];
    }
    void set(std::string const& db, std::string const& table, 
             std::string const& alias) {
        _map[makeKey(db, table)] = alias;
    }
    inline std::string makeKey(std::string db, std::string table) {
        std::stringstream ss;
        ss << db << "__" << table << "__";
        return ss.str();
    }
    std::map<std::string, std::string> _map;
};

class addMap {
public:
    explicit addMap(ReverseAlias& r) : _reverseAlias(r) {}
    void operator()(std::string const& alias, 
                    std::string const& db, std::string const& table) {
        // std::cout << "set: " << alias << "->" 
        //           << db << "." << table << std::endl;
        _reverseAlias.set(db, table, alias);
    }
    ReverseAlias& _reverseAlias;
};

class generateAlias {
public:
    generateAlias(int& seqN) : _seqN(seqN) {}
    std::string operator()() {
        std::stringstream ss;
        ss << "QST_" << ++_seqN << "_";
        return ss.str();
    }
    int& _seqN;
};
class addDbContext : public qMaster::TableRefN::Func {
public:
    addDbContext(qMaster::QueryContext const& c) : context(c) {}
    void operator()(qMaster::TableRefN::Ptr t) {
        if(t.get()) { t->apply(*this); }     
    }
    void operator()(qMaster::TableRefN& t) {
        std::string table = t.getTable();
        if(table.empty()) return; // Add context only to concrete refs
        if(t.getDb().empty()) { t.setDb(context.defaultDb); }
        if(dominantDb.empty()) { dominantDb = t.getDb(); }
    }
    qMaster::QueryContext const& context;
    std::string dominantDb;
};

template <typename G, typename A>
class addAlias {
public:
    addAlias(G g, A a) : _generate(g), _addMap(a) {}
    void operator()(qMaster::TableRefN::Ptr t) {
        // std::cout << "tableref:";
        // t->putStream(std::cout);
        // std::cout << std::endl;
        // If no alias, then add one. 
        std::string alias = t->getAlias();
        if(alias.empty()) {
            alias = _generate();
            t->setAlias(alias);
        }
        // Save ref
        _addMap(alias, t->getDb(), t->getTable());
    }
private:
    G _generate; // Functor that creates a new alias name
    A _addMap; // Functor that adds a new alias mapping for matchin
               // later clauses.
};
} // anonymous

namespace lsst { namespace qserv { namespace master {

////////////////////////////////////////////////////////////////////////
// fixExprAlias is a functor that acts on ValueExpr objects and
// modifys them in-place, altering table names to use an aliased name
// that is mapped via ReverseAlias. 
// It does not add table qualifiers where none already exist, because
// there is no compelling reason to do so (yet).
////////////////////////////////////////////////////////////////////////
class fixExprAlias {
public:
    explicit fixExprAlias(ReverseAlias& r) : _reverseAlias(r) {}
    void operator()(ValueExprPtr& vep) {
        if(!vep.get()) {
            return;
        }
        // For each factor in the expr, patch for aliasing:
        ValueExpr::FactorOpList& factorOps = vep->getFactorOps();
        for(ValueExpr::FactorOpList::iterator i=factorOps.begin();
            i != factorOps.end(); ++i) {
            if(!i->factor) {
                throw std::logic_error("Bad ValueExpr::FactorOps");
            }
            ValueFactor& t = *i->factor;
            //std::cout << "fixing factor: " << *vep << std::endl;
            std::string newAlias;
            
            switch(t.getType()) {
            case ValueFactor::COLUMNREF:
                // check columnref.
                _patchColumnRef(*t.getColumnRef());
                break;
            case ValueFactor::FUNCTION:
            case ValueFactor::AGGFUNC:
                // recurse for func params (aggfunc is special case of function)
                _patchFuncExpr(*t.getFuncExpr());
                break;
            case ValueFactor::STAR: 
                // Patch db/table name if applicable
                _patchStar(t);
                break;
            default: break;
            }
        }
    }
private:
    inline void _patchColumnRef(ColumnRef& ref) {
        std::string newAlias = _getAlias(ref.db, ref.table);
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        // Eliminate db. Replace table with aliased table.
        ref.db.assign("");
        ref.table.assign(newAlias);
    }
    inline void _patchFuncExpr(FuncExpr& fe) {
        std::for_each(fe.params.begin(), fe.params.end(), 
                      fixExprAlias(_reverseAlias));
    }
    inline void _patchStar(ValueFactor& vt) {
        // TODO: No support for <db>.<table>.* in framework
        // Only <table>.* is supported.
        std::string newAlias = _getAlias("", vt.getTableStar());
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        else { vt.setTableStar(newAlias); }
    }


    inline std::string _getAlias(std::string const& db,
                                 std::string const& table) {
        //std::cout << "lookup: " << db << "." << table << std::endl;
        return _reverseAlias.get(db, table);
    }
    ReverseAlias& _reverseAlias;
};

////////////////////////////////////////////////////////////////////////
// TablePlugin declaration
////////////////////////////////////////////////////////////////////////
/// TablePlugin is a query plugin that inserts placeholders for table
/// name substitution.
class TablePlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<TablePlugin> Ptr;
    
    virtual ~TablePlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(SelectStmt& stmt, QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, QueryContext& context);
private:
    std::string _dominantDb;
};

////////////////////////////////////////////////////////////////////////
// TablePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class TablePluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<TablePluginFactory> Ptr;
    TablePluginFactory() {}
    virtual ~TablePluginFactory() {}

    virtual std::string getName() const { return "Table"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new TablePlugin());
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
}

////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void 
TablePlugin::applyLogical(SelectStmt& stmt, QueryContext& context) {
    // Idea: Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    FromList& fList = stmt.getFromList();
    // std::cout << "TABLE:Logical:orig fromlist "
    //           << fList.getGenerated() << std::endl;
    TableRefnList& tList = fList.getTableRefnList();
    
    // For each tableref, modify to add alias.
    int seq=0;
    ReverseAlias reverseAlias;
    std::for_each(tList.begin(), tList.end(), 
                  addAlias<generateAlias,addMap>(generateAlias(seq), 
                                                 addMap(reverseAlias)));
    // FIXME: Now is a good time to verify that access is allowed and
    // populate the context accordingly.  

    // Now snoop around the other clauses (SELECT, WHERE, etc. and
    // patch their table references)
    // select list
    SelectList& sList = stmt.getSelectList();
    ValueExprList& exprList = *sList.getValueExprList();
    std::for_each(exprList.begin(), exprList.end(), 
                  fixExprAlias(reverseAlias));
    // where
    if(stmt.hasWhereClause()) {
        WhereClause& wClause = stmt.getWhereClause();
        WhereClause::ValueExprIter veI = wClause.vBegin();
        WhereClause::ValueExprIter veEnd = wClause.vEnd();
        std::for_each(veI, veEnd, fixExprAlias(reverseAlias));
    }
    // Fill-in default db context.
    addDbContext adc(context);
    std::for_each(tList.begin(), tList.end(), adc);
    _dominantDb = adc.dominantDb;
    if(_dominantDb.empty()) {
        _dominantDb = context.defaultDb;
    }
    
    // Apply function using the iterator...
    //wClause.walk(fixExprAlias(reverseAlias));
    // order by
    // having        
    context.dominantDb = _dominantDb;
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p, QueryContext& context) {
    // For each entry in original's SelectList, modify the SelectList
    // for the parallel and merge versions.
    // Set hasMerge to true if aggregation is detected.
    SelectList& oList = p.stmtOriginal.getSelectList();
    boost::shared_ptr<qMaster::ValueExprList> vlist;
    vlist = oList.getValueExprList();
    if(!vlist) {
        throw std::logic_error("Invalid stmtOriginal.SelectList");
    }
    p.dominantDb = _dominantDb;
    

    // Idea: Rewrite table names in from-list of the parallel
    // query. This is sufficient because table aliases were added in
    // the logical plugin stage so that real table refs should only
    // exist in the from-list.
    FromList& fList = p.stmtParallel.getFromList();
    //    std::cout << "orig fromlist " << fList.getGenerated() << std::endl;

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
    SphericalBoxStrategy s(fList, context);
    QueryMapping::Ptr qm = s.getMapping();
    s.patchFromList(fList);
    // std::cout << "post-patched fromlist " << fList.getGenerated() << std::endl;

    // Now add/merge the mapping to the Plan
    if(!p.queryMapping.get()) {
        p.queryMapping = qm;
    } else {
        p.queryMapping->update(*qm);
    }
    // Query generation needs to be sensitive to this.
    // If no subchunks are needed, 
    
    //
    // For each tableref, modify to replace tablename with
    // substitutable.
    


}
}}} // namespace lsst::qserv::master
