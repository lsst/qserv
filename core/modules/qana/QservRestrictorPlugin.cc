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
  * @brief QservRestrictorPlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// No public interface (no QservRestrictorPlugin.h)
// Parent class
#include "qana/QueryPlugin.h"

// System headers
#include <algorithm>
#include <deque>
#include <memory>
#include <string>

// Third-party headers
#include "boost/pointer_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssAccess.h"
#include "global/stringTypes.h"
#include "qana/AnalysisError.h"
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

    LOG_LOGGER getLogger() {
        static LOG_LOGGER logger = LOG_GET("lsst.qserv.qana.QservRestrictorPlugin");
        return logger;
    }
} // anonymous

namespace lsst {
namespace qserv {
namespace qana {

typedef std::pair<std::string, std::string> StringPair;

std::shared_ptr<query::ColumnRef>
resolveAsColumnRef(query::QueryContext& context, query::ValueExprPtr vexpr) {
    std::shared_ptr<query::ColumnRef> cr = vexpr->copyAsColumnRef();
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
               std::shared_ptr<query::ColumnRef> cr) {
    // Match cr as a column ref against the secondary index column for a
    // database's partitioning strategy.
    if((!cr) || !context.css) { return false; }
    if(!context.css->containsDb(cr->db)
       || !context.css->containsTable(cr->db, cr->table)) {
        throw AnalysisError("Invalid db/table:" + cr->db + "." + cr->table);
    }
    if (cr->column.empty()) {
        return false;
    }
    std::vector<std::string> sics = context.css->getPartTableParams(
        cr->db, cr->table).secIndexColNames();
    return std::find(sics.begin(), sics.end(), cr->column) != sics.end();
}

query::PassTerm::Ptr newPass(std::string const& s) {
    query::PassTerm::Ptr p = std::make_shared<query::PassTerm>();
    p->_text = s;
    return p;
}
template <typename C>
query::PassListTerm::Ptr newPassList(C& c) {
    query::PassListTerm::Ptr p = std::make_shared<query::PassListTerm>();
    p->_terms.insert(p->_terms.begin(), c.begin(), c.end());
    return p;
}

query::InPredicate::Ptr
newInPred(std::string const& aliasTable,
          std::string const& secIndexColumn,
          std::vector<std::string> const& params) {
    query::InPredicate::Ptr p = std::make_shared<query::InPredicate>();
    std::shared_ptr<query::ColumnRef> cr =
               std::make_shared<query::ColumnRef>(
                       "", aliasTable, secIndexColumn);
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
    query::FuncExpr::Ptr fe = std::make_shared<query::FuncExpr>();
    fe->name = UDF_PREFIX + fName;
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                  std::make_shared<query::ColumnRef>(
                          "", tableAlias, chunkColumns.first))));
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                  std::make_shared<query::ColumnRef>(
                          "", tableAlias, chunkColumns.second))));

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

    getTable(css::CssAccess& css, RestrictorEntries& entries)
        : _css(css),
          _entries(entries) {}

    void operator()(query::TableRef::Ptr t) {
        // FIXME: Modify so we can use TableRef::apply()
        if(!t) {
            throw AnalysisBug("NULL TableRefN::Ptr");
        }
        (*this)(*t);
    }
    virtual void operator()(query::TableRef& t) {
        std::string const& db = t.getDb();
        std::string const& table = t.getTable();

        if(db.empty()
           || !_css.containsDb(db)
           || !_css.containsTable(db, table)) {
            throw AnalysisError("Invalid db/table:" + db + "." + table);
        }
        css::PartTableParams const& partParam = _css.getPartTableParams(db, table);
        // Is table chunked?
        if(!partParam.isChunked()) {
            return; // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t.getAlias();
        if(alias.empty()) {
            // For now, only accept aliased tablerefs (should have
            // been done earlier)
            throw AnalysisBug("Unexpected unaliased table reference");
        }
        std::vector<std::string> pCols = partParam.partitionCols();
        RestrictorEntry se(alias,
                           StringPair(pCols[0], pCols[1]),
                           pCols[2]);
        _entries.push_back(se);
        query::JoinRefPtrVector& joinRefs = t.getJoins();
        typedef query::JoinRefPtrVector::iterator Iter;
        for(Iter i=joinRefs.begin(), e=joinRefs.end(); i != e; ++i) {
            (*this)((**i).getRight());
        }
    }
    css::CssAccess& _css;
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
    typedef std::shared_ptr<QservRestrictorPlugin> Ptr;
    class Restriction;

    virtual ~QservRestrictorPlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context);

private:
    query::BoolTerm::Ptr
        _makeCondition(std::shared_ptr<query::QsRestrictor> const restr,
                       RestrictorEntry const& restrictorEntry);
    std::shared_ptr<query::QsRestrictor::PtrVector>
        _getSecIndexPreds(query::QueryContext&,
                          query::AndTerm::Ptr);
    query::QsRestrictor::Ptr
        _newSecIndexRestrictor(query::QueryContext& context,
                               std::shared_ptr<query::ColumnRef> cr,
                               query::ValueExprPtrVector& vList);
    query::QsRestrictor::Ptr
        _newSecIndexRestrictor(query::QueryContext& context,
                               std::shared_ptr<query::CompPredicate> cp);
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
        ObjectIdGenerator(StringVector const& params_)
            : params(params_.begin(), params_.end()) {
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor =
                    std::make_shared<query::BoolFactor>();
            query::BfTerm::PtrVector& terms = newFactor->_terms;
            terms.push_back(newInPred(e.alias, e.secIndexColumn, params));
            return newFactor;
        }
        std::vector<std::string> params;
    };

    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      StringVector const& params_)
            :  fName(fName_), paramCount(paramCount_), params(params_) {
            if(paramCount_ == USE_STRING) {
                // Convert param list to one quoted string.
                // This bundles a variable-sized list into a single
                // parameter to work with the MySQL UDF facility.
            }
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor =
                    std::make_shared<query::BoolFactor>();
            query::BfTerm::PtrVector& terms = newFactor->_terms;
            query::CompPredicate::Ptr cp =
                    std::make_shared<query::CompPredicate>();
            std::shared_ptr<query::FuncExpr> fe =
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
        StringVector const& params;
        static const int USE_STRING = -999;
    };

    void _setGenerator(query::QsRestrictor const& r) {
        if(r._name == "qserv_areaspec_box") {
            _generator = std::make_shared<AreaGenerator>("s2PtInBox",
                                                         4,
                                                         r._params
                                                         );
        } else if(r._name == "qserv_areaspec_circle") {
            _generator = std::make_shared<AreaGenerator>("s2PtInCircle",
                                                         3,
                                                         r._params
                                                         );
        } else if(r._name == "qserv_areaspec_ellipse") {
            _generator = std::make_shared<AreaGenerator>("s2PtInEllipse",
                                                         5,
                                                         r._params
                                                         );
        } else if(r._name == "qserv_areaspec_poly") {
            const int use_string = AreaGenerator::USE_STRING;
            _generator = std::make_shared<AreaGenerator>("s2PtInCPoly",
                                                         use_string,
                                                         r._params
                                                         );
        } else if(_name == "qserv_objectId") {
            _generator = std::make_shared<ObjectIdGenerator>(r._params);
        } else {
            throw AnalysisBug("Unmatched restriction spec: " + _name);
        }
    }
    std::string _name;
    std::vector<double> _params;
    std::shared_ptr<Generator> _generator;
};


////////////////////////////////////////////////////////////////////////
// QservRestrictorPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class QservRestrictorPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef std::shared_ptr<QservRestrictorPluginFactory> Ptr;
    QservRestrictorPluginFactory() {}
    virtual ~QservRestrictorPluginFactory() {}

    virtual std::string getName() const { return "QservRestrictor"; }
    virtual QueryPlugin::Ptr newInstance() {
        return std::make_shared<QservRestrictorPlugin>();
    }
};

////////////////////////////////////////////////////////////////////////
// registerQservRestrictorPlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        QservRestrictorPluginFactory::Ptr f = std::make_shared<QservRestrictorPluginFactory>();
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
    if(!context.css) {
        throw AnalysisBug("Missing metadata in context");
    }
    getTable gt(*context.css, entries);
    std::for_each(tList.begin(), tList.end(), gt);

    if(!stmt.hasWhereClause()) { return; }

    // Prepare to patch the WHERE clause
    query::WhereClause& wc = stmt.getWhereClause();

    std::shared_ptr<query::QsRestrictor::PtrVector const> restrsPtr = wc.getRestrs();
    query::AndTerm::Ptr originalAnd(wc.getRootAndTerm());
    std::shared_ptr<query::QsRestrictor::PtrVector> secIndexPreds =
        _getSecIndexPreds(context, originalAnd);
    query::AndTerm::Ptr newTerm;
    // Now handle the explicit restrictors
    if(restrsPtr && !restrsPtr->empty()) {
        // spatial restrictions
        query::QsRestrictor::PtrVector const& restrs = *restrsPtr;
        context.restrictors = std::make_shared<query::QueryContext::RestrList>();
        newTerm = std::make_shared<query::AndTerm>();

        // At least one table should exist in the restrictor entries
        if(entries.empty()) {
            throw AnalysisError("Spatial restrictor w/o partitioned table");
        }

        // Now, for each of the qserv restrictors:
        for(query::QsRestrictor::PtrVector::const_iterator i=restrs.begin();
            i != restrs.end(); ++i) {
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
             std::shared_ptr<query::QsRestrictor> const restr,
             RestrictorEntry const& restrictorEntry) {
    Restriction r(*restr);
    return r.generate(restrictorEntry);
}

inline void
addPred(std::shared_ptr<query::QsRestrictor::PtrVector>& preds,
        query::QsRestrictor::Ptr p) {
    if(p) {
        if(!preds) {
            preds = std::make_shared<query::QsRestrictor::PtrVector>();
        }
        preds->push_back(p);
    }
}

std::shared_ptr<query::QsRestrictor::PtrVector>
QservRestrictorPlugin::_getSecIndexPreds(query::QueryContext& context,
                                         query::AndTerm::Ptr andTerm) {
    typedef query::BoolTerm::PtrVector::iterator TermIter;
    typedef query::BfTerm::PtrVector::iterator BfIter;
    std::shared_ptr<query::QsRestrictor::PtrVector> secIndexPreds;

    if(not andTerm) return nullptr;

    for(TermIter i = andTerm->iterBegin(); i != andTerm->iterEnd(); ++i) {
        query::BoolFactor* factor = dynamic_cast<query::BoolFactor*>(i->get());
        if(!factor) continue;
        for(BfIter b = factor->_terms.begin();
            b != factor->_terms.end();
            ++b) {

            // IN predicate
            query::InPredicate::Ptr inPredicate =
                std::dynamic_pointer_cast<query::InPredicate>(*b);
            if(inPredicate) {
                std::shared_ptr<query::ColumnRef> cr
                    = resolveAsColumnRef(context, inPredicate->value);
                if(cr && lookupSecIndex(context, cr)) {
                    query::QsRestrictor::Ptr p =
                        _newSecIndexRestrictor(context, cr, inPredicate->cands);
                    addPred(secIndexPreds, p);
                }
                continue;
            }

            // '=' predicate
            query::CompPredicate::Ptr compPredicate = std::dynamic_pointer_cast<query::CompPredicate>(*b);
            if(compPredicate) {
                query::QsRestrictor::Ptr p = _newSecIndexRestrictor(context, compPredicate);
                addPred(secIndexPreds, p);
                continue;
            }

            // BETWEEN predicate
            /* TODO
            query::BetweenPredicate::Ptr betweenPredicate = std::dynamic_pointer_cast<query::BetweenPredicate>(*b);
            if(betweenPredicate) {
                query::QsRestrictor::Ptr p = _newSecIndexRestrictor(context, betweenPredicate);
                addPred(secIndexPreds, p);
                continue;
            }
            */
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
    std::shared_ptr<query::ColumnRef> cr,
    query::ValueExprPtrVector& vList)
{
    // Extract the literals, bailing out if we see a non-literal
    bool isValid = true;
    std::for_each(vList.begin(), vList.end(), validateLiteral(isValid));
    if(!isValid) {
        return query::QsRestrictor::Ptr();
    }

    // Build the QsRestrictor
    query::QsRestrictor::Ptr p = std::make_shared<query::QsRestrictor>();
    p->_name = "sIndex";
    // sIndex has paramers as follows:
    // db, table, column, val1, val2, ...

    css::PartTableParams const partParam = context.css->getPartTableParams(cr->db, cr->table);
    // Get the director column name
    std::string dirCol = partParam.dirColName;
    if (cr->column == dirCol) {
        // cr may be a column in a child table, in which case we must figure
        // out the corresponding column in the child's director to properly
        // generate a secondary index constraint.
        std::string dirDb = partParam.dirDb;
        std::string dirTable = partParam.dirTable;
        if (dirTable.empty()) {
            dirTable = cr->table;
            if (!dirDb.empty() && dirDb != cr->db) {
                LOGF_ERROR("dirTable missing, but dirDb is set inconsistently for %1%.%2%"
                           % cr->db % cr->table);
                return query::QsRestrictor::Ptr();
            }
            dirDb = cr->db;
        } else if (dirDb.empty()) {
            dirDb = cr->db;
        }
        if (dirDb != cr->db || dirTable != cr->table) {
            // Lookup the name of the director column in the director table
            dirCol = context.css->getPartTableParams(dirDb, dirTable).dirColName;
            if (dirCol.empty()) {
                LOGF_ERROR("dirCol missing for %1%.%2%" % dirDb % dirTable);
                return query::QsRestrictor::Ptr();
            }
        }
        LOGF_DEBUG("Using dirDb %1%, dirTable %2%, dirCol %3% as sIndex for %4%.%5%.%6%" %
                   dirDb % dirTable % dirCol % cr->db % cr->table % cr->column);
        p->_params.push_back(dirDb);
        p->_params.push_back(dirTable);
        p->_params.push_back(dirCol);
    } else {
        LOGF_DEBUG("Using %1%, %2%, %3% as sIndex." % cr->db % cr->table % cr->column);
        p->_params.push_back(cr->db);
        p->_params.push_back(cr->table);
        p->_params.push_back(cr->column);
    }
    std::transform(vList.begin(), vList.end(),
                   std::back_inserter(p->_params), extractLiteral());
    return p;
}

/// @return a new QsRestrictor from a CompPredicate
query::QsRestrictor::Ptr
QservRestrictorPlugin::_newSecIndexRestrictor(
    query::QueryContext& context,
    std::shared_ptr<query::CompPredicate> cp)
{
    query::QsRestrictor::Ptr p;
    std::shared_ptr<query::ColumnRef> secIndex =
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
    std::vector<std::shared_ptr<query::ValueExpr> > cands;
    cands.push_back(literalValue);
    return _newSecIndexRestrictor(context, secIndex, cands);
}

query::QsRestrictor::Ptr
QservRestrictorPlugin::_convertObjectId(query::QueryContext& context,
                                        query::QsRestrictor const& original) {
    // Build the QsRestrictor
    query::QsRestrictor::Ptr p = std::make_shared<query::QsRestrictor>();
    p->_name = "sIndex";
    // sIndex has paramers as follows:
    // db, table, column, val1, val2, ...
    p->_params.push_back(context.dominantDb);
    p->_params.push_back(context.anonymousTable);
    if(!context.css->containsDb(context.dominantDb)
       || !context.css->containsTable(context.dominantDb,
                                            context.anonymousTable) ) {
        throw AnalysisError("Invalid db/table: " + context.dominantDb
                            + "." + context.anonymousTable);
    }
    // TODO: The qserv_objectId hint/restrictor should be removed.
    // For now, assume that "objectId" refers to the director column.
    std::string dirColumn = context.css->getPartTableParams(
        context.dominantDb, context.anonymousTable).dirColName;
    p->_params.push_back(dirColumn);
    std::copy(original._params.begin(), original._params.end(),
              std::back_inserter(p->_params));
    return p;
}

}}} // namespace lsst::qserv::qana
