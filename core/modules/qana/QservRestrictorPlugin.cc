// -*- LSST-C++ -*-
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
  * @file
  *
  * @brief QservRestrictorPlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// No public interface (no QservRestrictorPlugin.h)

// System headers
#include <deque>
#include <string>

// Third-party headers
#include <boost/pointer_cast.hpp>

// Local headers
#include "css/Facade.h"
#include "qana/AnalysisError.h"
#include "qana/QueryPlugin.h" // Parent class
#include "query/ColumnRef.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/JoinRef.h"
#include "query/Predicate.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/SqlSQL2Tokens.h" // (generated) SqlSQL2TokenTypes
#include "query/ValueFactor.h"
#include "query/ValueExpr.h"
#include "query/WhereClause.h"


namespace { // File-scope helpers
    std::string const UDF_PREFIX = "scisql_";
} // anonymous

namespace lsst {
namespace qserv {
namespace qana {

typedef std::pair<std::string, std::string> StringPair;

boost::shared_ptr<query::ColumnRef>
resolveAsColumnRef(query::QueryContext& context, query::ValueExprPtr vexpr) {
    boost::shared_ptr<query::ColumnRef> cr = vexpr->copyAsColumnRef();
    if(!cr) {
        return cr;
    }
    query::DbTablePair p = context.resolve(cr);
    cr->table = p.table;
    cr->db = p.db;
    return cr;
}

/// @return true if cr represents a valid secondary index column.
bool
lookupSecIndex(query::QueryContext& context,
               boost::shared_ptr<query::ColumnRef> cr) {
    // Match cr as a column ref against the secondary index column for a
    // database's partitioning strategy.
    if((!cr) || !context.cssFacade) { return false; }
    if(!context.cssFacade->containsDb(cr->db)
       || !context.cssFacade->containsTable(cr->db, cr->table)) {
        throw qana::AnalysisError("Invalid db/table:"
                                  + cr->db + "." + cr->table);
        }
    if (cr->column.empty()) {
        return false;
    }
    std::vector<std::string> sics = context.cssFacade->getSecIndexColNames(
        cr->db, cr->table);
    return std::find(sics.begin(), sics.end(), cr->column) != sics.end();
}

query::PassTerm::Ptr newPass(std::string const& s) {
    query::PassTerm::Ptr p(new query::PassTerm);
    p->_text = s;
    return p;
}
template <typename C>
query::PassListTerm::Ptr newPassList(C& c) {
    query::PassListTerm::Ptr p(new query::PassListTerm);
    p->_terms.insert(p->_terms.begin(), c.begin(), c.end());
    return p;
}

query::InPredicate::Ptr
newInPred(std::string const& aliasTable,
          std::string const& secIndexColumn,
          std::vector<std::string> const& params) {
    query::InPredicate::Ptr p(new query::InPredicate());
    boost::shared_ptr<query::ColumnRef> cr(
               new query::ColumnRef("", aliasTable, secIndexColumn));
    p->value =
        query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(cr));

    typedef std::vector<std::string>::const_iterator Iter;
    for(Iter i=params.begin(), e=params.end(); i != e; ++i) {
        query::ValueExprPtr vep;
        vep = query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(*i));
        p->cands.push_back(vep);
    }
    return p;
}

template <typename C>
query::FuncExpr::Ptr newFuncExpr(char const fName[],
                                 std::string const& tableAlias,
                                 StringPair const& chunkColumns,
                                 C& c) {
    typedef boost::shared_ptr<query::ColumnRef> CrPtr;
    query::FuncExpr::Ptr fe(new query::FuncExpr);
    fe->name = UDF_PREFIX + fName;
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
          CrPtr(new query::ColumnRef("", tableAlias, chunkColumns.first)))));
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
          CrPtr(new query::ColumnRef("", tableAlias, chunkColumns.second)))));

    typename C::const_iterator i;
    for(i = c.begin(); i != c.end(); ++i) {
        fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(*i)));
    }
    return fe;
}

struct RestrictorEntry {
    RestrictorEntry(std::string const& alias_,
                 StringPair const& chunkColumns_,
                 std::string const& secIndexColumn_)
        : alias(alias_),
          chunkColumns(chunkColumns_),
          secIndexColumn(secIndexColumn_)
        {}
    std::string alias;
    StringPair chunkColumns;
    std::string secIndexColumn;
};
typedef std::deque<RestrictorEntry> RestrictorEntries;
class getTable : public query::TableRef::Func {
public:

    getTable(css::Facade& cssFacade, RestrictorEntries& entries)
        : _cssFacade(cssFacade),
          _entries(entries) {}

    void operator()(query::TableRef::Ptr t) {
        // FIXME: Modify so we can use TableRef::apply()
        if(!t) {
            throw qana::AnalysisBug("NULL TableRefN::Ptr");
        }
        (*this)(*t);
    }
    virtual void operator()(query::TableRef& t) {
        std::string const& db = t.getDb();
        std::string const& table = t.getTable();

        if(db.empty()
           || !_cssFacade.containsDb(db)
           || !_cssFacade.containsTable(db, table)) {
            throw qana::AnalysisError("Invalid db/table:" + db + "." + table);
        }
        // Is table chunked?
        if(!_cssFacade.tableIsChunked(db, table)) {
            return; // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t.getAlias();
        if(alias.empty()) {
            // For now, only accept aliased tablerefs (should have
            // been done earlier)
            throw qana::AnalysisBug("Unexpected unaliased table reference");
        }
        std::vector<std::string> pCols = _cssFacade.getPartitionCols(db, table);
        RestrictorEntry se(alias,
                           StringPair(pCols[0], pCols[1]),
                           pCols[2]);
        _entries.push_back(se);
        query::JoinRefList& jList = t.getJoins();
        typedef query::JoinRefList::iterator Iter;
        for(Iter i=jList.begin(), e=jList.end(); i != e; ++i) {
            (*this)((**i).getRight());
        }
    }
    css::Facade& _cssFacade;
    RestrictorEntries& _entries;
};
////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin declaration
////////////////////////////////////////////////////////////////////////
/// QservRestrictorPlugin replaces a qserv restrictor spec with directives
/// that can be executed on a qserv mysqld. This plugin should be
/// execute after aliases for tables have been generates, so that the
/// new restrictor function clauses/phrases can use the aliases.
class QservRestrictorPlugin : public QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<QservRestrictorPlugin> Ptr;
    class Restriction;

    virtual ~QservRestrictorPlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context);

private:
    query::BoolTerm::Ptr
        _makeCondition(boost::shared_ptr<query::QsRestrictor> const restr,
                       RestrictorEntry const& restrictorEntry);
    boost::shared_ptr<query::QsRestrictor::List>
        _getSecIndexPreds(query::QueryContext&,
                          query::AndTerm::Ptr);
    query::QsRestrictor::Ptr
        _newSecIndexRestrictor(query::QueryContext& context,
                               boost::shared_ptr<query::ColumnRef> cr,
                               query::ValueExprList& vList);
    query::QsRestrictor::Ptr
        _newSecIndexRestrictor(query::QueryContext& context,
                               boost::shared_ptr<query::CompPredicate> cp);
    query::QsRestrictor::Ptr
        _convertObjectId(query::QueryContext& context,
                         query::QsRestrictor const& original);
};

////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin::Restriction
// Generates WHERE clause terms from restriction specs. Borrowed from
// older parsing framework.
////////////////////////////////////////////////////////////////////////
class QservRestrictorPlugin::Restriction {
public:
    Restriction(query::QsRestrictor const& r)
        : _name(r._name) {
        _setGenerator(r);
    }
    query::BoolFactor::Ptr generate(RestrictorEntry const& e) {
        return (*_generator)(e);
    }

    // std::string getUdfCallString(std::string const& tName,
    //                              StringMap const& tableConfig) const {
    //     if(_generator.get()) {
    //         return (*_generator)(tName, tableConfig);
    //     }
    //     return std::string();
    // }
    class Generator {
    public:
        virtual ~Generator() {}
        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) = 0;
    };
private:
    class ObjectIdGenerator : public Generator {
    public:
        ObjectIdGenerator(query::QsRestrictor::StringList const& params_)
            : params(params_.begin(), params_.end()) {
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor(new query::BoolFactor);
            query::BfTerm::PtrList& terms = newFactor->_terms;
            terms.push_back(newInPred(e.alias, e.secIndexColumn, params));
            return newFactor;
        }
        std::vector<std::string> params;
    };

    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      query::QsRestrictor::StringList const& params_)
            :  fName(fName_), paramCount(paramCount_), params(params_) {
            if(paramCount_ == USE_STRING) {
                // Convert param list to one quoted string.
                // This bundles a variable-sized list into a single
                // parameter to work with the MySQL UDF facility.
            }
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor(new query::BoolFactor);
            query::BfTerm::PtrList& terms = newFactor->_terms;
            query::CompPredicate::Ptr cp(new query::CompPredicate());
            boost::shared_ptr<query::FuncExpr> fe =
                newFuncExpr(fName, e.alias, e.chunkColumns, params);
            cp->left =
                query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(fe));
            cp->op = SqlSQL2Tokens::EQUALS_OP;
            cp->right = query::ValueExpr::newSimple(
                           query::ValueFactor::newConstFactor("1"));
            terms.push_back(cp);
            return newFactor;
        }
        char const* const fName;
        int const paramCount;
        query::QsRestrictor::StringList const& params;
        static const int USE_STRING = -999;
    };

    void _setGenerator(query::QsRestrictor const& r) {
        if(r._name == "qserv_areaspec_box") {
            _generator.reset(static_cast<Generator*>
                             (new AreaGenerator("s2PtInBox",
                                                4, r._params)));
        } else if(r._name == "qserv_areaspec_circle") {
            _generator.reset(static_cast<Generator*>
                             (new AreaGenerator("s2PtInCircle",
                                                3, r._params)));
        } else if(r._name == "qserv_areaspec_ellipse") {
            _generator.reset(static_cast<Generator*>
                             (new AreaGenerator("s2PtInEllipse",
                                                5, r._params)));
        } else if(r._name == "qserv_areaspec_poly") {
            _generator.reset(static_cast<Generator*>
                             (new AreaGenerator("s2PtInCPoly",
                                                AreaGenerator::USE_STRING,
                                                r._params)));
        } else if(_name == "qserv_objectId") {
            ObjectIdGenerator* g = new ObjectIdGenerator(r._params);
            _generator.reset(static_cast<Generator*>(g));
        } else {
            throw qana::AnalysisBug("Unmatched restriction spec: " + _name);
        }
    }
    std::string _name;
    std::vector<double> _params;
    boost::shared_ptr<Generator> _generator;
};


////////////////////////////////////////////////////////////////////////
// QservRestrictorPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class QservRestrictorPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<QservRestrictorPluginFactory> Ptr;
    QservRestrictorPluginFactory() {}
    virtual ~QservRestrictorPluginFactory() {}

    virtual std::string getName() const { return "QservRestrictor"; }
    virtual QueryPlugin::Ptr newInstance() {
        return QueryPlugin::Ptr(new QservRestrictorPlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registerQservRestrictorPlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        QservRestrictorPluginFactory::Ptr f(new QservRestrictorPluginFactory());
        QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerQservRestrictorPlugin;
}

////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin implementation
////////////////////////////////////////////////////////////////////////
void
QservRestrictorPlugin::applyLogical(query::SelectStmt& stmt,
                                    query::QueryContext& context) {
    // Idea: For each of the qserv restrictors in the WHERE clause,
    // rewrite in the context of whatever chunked tables exist in the
    // FROM list.

    // First, get a list of the chunked tables.
    query::FromList& fList = stmt.getFromList();
    query::TableRefList& tList = fList.getTableRefList();
    RestrictorEntries entries;
    if(!context.cssFacade) {
        throw qana::AnalysisBug("Missing metadata in context");
    }
    getTable gt(*context.cssFacade, entries);
    std::for_each(tList.begin(), tList.end(), gt);

    if(!stmt.hasWhereClause()) { return; }

    // Prepare to patch the WHERE clause
    query::WhereClause& wc = stmt.getWhereClause();

    boost::shared_ptr<query::QsRestrictor::List const> rListP = wc.getRestrs();
    query::AndTerm::Ptr originalAnd(wc.getRootAndTerm());
    boost::shared_ptr<query::QsRestrictor::List> secIndexPreds =
        _getSecIndexPreds(context, originalAnd);
    query::AndTerm::Ptr newTerm;
    // Now handle the explicit restrictors
    if(rListP && !rListP->empty()) {
        // spatial restrictions
        query::QsRestrictor::List const& rList = *rListP;
        context.restrictors.reset(new query::QueryContext::RestrList);
        newTerm.reset(new query::AndTerm);

        // Now, for each of the qserv restrictors:
        for(query::QsRestrictor::List::const_iterator i=rList.begin();
            i != rList.end(); ++i) {
            // for each restrictor entry
            // generate a restrictor condition.
            for(RestrictorEntries::const_iterator j = entries.begin();
                j != entries.end(); ++j) {
                newTerm->_terms.push_back(_makeCondition(*i, *j));
            }
            if((**i)._name == "qserv_objectId") {
                // Convert to secIndex restrictor
                query::QsRestrictor::Ptr p = _convertObjectId(context, **i);
                context.restrictors->push_back(p);
            } else {
                // Save restrictor in QueryContext.
                context.restrictors->push_back(*i);
            }
        }
    }
    wc.resetRestrs();
    // Merge in the implicit restrictors
    if(secIndexPreds) {
        if(!context.restrictors) {
            context.restrictors = secIndexPreds;
        } else {
            context.restrictors->insert(context.restrictors->end(),
                                        secIndexPreds->begin(),
                                        secIndexPreds->end());
        }
    }
    if(context.restrictors && context.restrictors->empty()) {
        context.restrictors.reset();
    }
    if(newTerm) { wc.prependAndTerm(newTerm); }
}

void
QservRestrictorPlugin::applyPhysical(QueryPlugin::Plan& p,
                                     query::QueryContext& context) {
    // Probably nothing is needed here...
}

query::BoolTerm::Ptr
QservRestrictorPlugin::_makeCondition(
             boost::shared_ptr<query::QsRestrictor> const restr,
             RestrictorEntry const& restrictorEntry) {
    Restriction r(*restr);
    return r.generate(restrictorEntry);
}

inline void
addPred(boost::shared_ptr<query::QsRestrictor::List>& preds,
        query::QsRestrictor::Ptr p) {
    if(p) {
        if(!preds) {
            preds.reset(new query::QsRestrictor::List());
        }
        preds->push_back(p);
    }
}

boost::shared_ptr<query::QsRestrictor::List>
QservRestrictorPlugin::_getSecIndexPreds(query::QueryContext& context,
                                         query::AndTerm::Ptr p) {
    typedef query::BoolTerm::PtrList::iterator TermIter;
    typedef query::BfTerm::PtrList::iterator BfIter;
    boost::shared_ptr<query::QsRestrictor::List> secIndexPreds;

    if(!p) return secIndexPreds;

    for(TermIter i = p->iterBegin(); i != p->iterEnd(); ++i) {
        query::BoolFactor* factor = dynamic_cast<query::BoolFactor*>(i->get());
        if(!factor) continue;
        for(BfIter b = factor->_terms.begin();
            b != factor->_terms.end();
            ++b) {
            query::InPredicate::Ptr ip =
                boost::dynamic_pointer_cast<query::InPredicate>(*b);
            if(ip) {
                boost::shared_ptr<query::ColumnRef> cr
                    = resolveAsColumnRef(context, ip->value);
                if(cr && lookupSecIndex(context, cr)) {
                    query::QsRestrictor::Ptr p =
                        _newSecIndexRestrictor(context, cr, ip->cands);
                    addPred(secIndexPreds, p);
                }
            } else {
                query::CompPredicate::Ptr cp =
                    boost::dynamic_pointer_cast<query::CompPredicate>(*b);
                if(cp) {
                    query::QsRestrictor::Ptr p = _newSecIndexRestrictor(context, cp);
                    addPred(secIndexPreds, p);
                }
            }
        }
    }
    return secIndexPreds;
}

inline bool isValidLiteral(query::ValueExprPtr p) {
    return p && !p->copyAsLiteral().empty();
}

struct validateLiteral {
    validateLiteral(bool& isValid_) : isValid(isValid_) {}
    inline void operator()(query::ValueExprPtr p) {
        isValid = isValid && isValidLiteral(p);
    }
    bool& isValid;
};

struct extractLiteral {
    inline std::string operator()(query::ValueExprPtr p) {
        return p->copyAsLiteral();
    }
};
/// @return a new QsRestrictor from the column ref and the set of
/// specified values or NULL if one of the values is a non-literal.
query::QsRestrictor::Ptr
QservRestrictorPlugin::_newSecIndexRestrictor(
    query::QueryContext& context,
    boost::shared_ptr<query::ColumnRef> cr,
    query::ValueExprList& vList)
{
    // Extract the literals, bailing out if we see a non-literal
    bool isValid = true;
    std::for_each(vList.begin(), vList.end(), validateLiteral(isValid));
    if(!isValid) {
        return query::QsRestrictor::Ptr();
    }

    // Build the QsRestrictor
    query::QsRestrictor::Ptr p(new query::QsRestrictor());
    p->_name = "sIndex";
    // sIndex has paramers as follows:
    // db, table, column, val1, val2, ...
    p->_params.push_back(cr->db);
    p->_params.push_back(cr->table);
    p->_params.push_back(cr->column);
    std::transform(vList.begin(), vList.end(),
                   std::back_inserter(p->_params), extractLiteral());
    return p;
}

/// @return a new QsRestrictor from a CompPredicate
query::QsRestrictor::Ptr
QservRestrictorPlugin::_newSecIndexRestrictor(
    query::QueryContext& context,
    boost::shared_ptr<query::CompPredicate> cp)
{
    query::QsRestrictor::Ptr p;
    boost::shared_ptr<query::ColumnRef> secIndex =
        resolveAsColumnRef(context, cp->left);
    int op = cp->op;
    query::ValueExprPtr literalValue = cp->right;
    // Find the secondary index column ref: Is it on the rhs or lhs?
    if(secIndex && lookupSecIndex(context, secIndex)) {
        // go on.
    } else {
        secIndex = resolveAsColumnRef(context, cp->right);
        if(secIndex && lookupSecIndex(context, secIndex)) {
            op = query::CompPredicate::reverseOp(op);
            literalValue = cp->left;
        } else {
            return p; // No secondary index column ref. Leave it alone.
        }
    }
    // Make sure the expected literal is a literal
    bool isValid = true;
    validateLiteral vl(isValid);
    vl(literalValue);
    if(!isValid) { return p; } // No secondary index. Leave alone.
    std::list<boost::shared_ptr<query::ValueExpr> > cands;
    cands.push_back(literalValue);
    return _newSecIndexRestrictor(context, secIndex, cands);
}

query::QsRestrictor::Ptr
QservRestrictorPlugin::_convertObjectId(query::QueryContext& context,
                                        query::QsRestrictor const& original) {
    // Build the QsRestrictor
    query::QsRestrictor::Ptr p(new query::QsRestrictor());
    p->_name = "sIndex";
    // sIndex has paramers as follows:
    // db, table, column, val1, val2, ...
    p->_params.push_back(context.dominantDb);
    p->_params.push_back(context.anonymousTable);
    if(!context.cssFacade->containsDb(context.dominantDb)
       || !context.cssFacade->containsTable(context.dominantDb,
                                            context.anonymousTable) ) {
        throw qana::AnalysisError("Invalid db/table: " + context.dominantDb
                                  + "." + context.anonymousTable);
    }
    // TODO: The qserv_objectId hint/restrictor should be removed.
    // For now, assume that "objectId" refers to the director column.
    std::string dirColumn = context.cssFacade->getDirColName(
        context.dominantDb, context.anonymousTable);
    p->_params.push_back(dirColumn);
    std::copy(original._params.begin(), original._params.end(),
              std::back_inserter(p->_params));
    return p;
}

}}} // namespace lsst::qserv::qana
