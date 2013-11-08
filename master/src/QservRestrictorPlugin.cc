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
  * @file
  *
  * @brief QservRestrictorPlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// No public interface (no QservRestrictorPlugin.h)
#include <deque>
#include <string>
#include <boost/pointer_cast.hpp>

#include "lsst/qserv/master/QueryPlugin.h" // Parent class
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/MetadataCache.h"
#include "lsst/qserv/master/Predicate.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/ValueFactor.h"
#include "lsst/qserv/master/ValueExpr.h"
#include "lsst/qserv/master/WhereClause.h"

#include "SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes


namespace { // File-scope helpers
std::string const UDF_PREFIX = "scisql_";
} // anonymous

namespace lsst {
namespace qserv {
namespace master {
typedef std::pair<std::string,std::string> StringPair;

boost::shared_ptr<ColumnRef>
resolveAsColumnRef(QueryContext& context, ValueExprPtr vexpr) {
    boost::shared_ptr<ColumnRef> cr = vexpr->copyAsColumnRef();
    if(!cr) {
        return cr;
    }
    DbTablePair p = context.resolve(cr);
    cr->table = p.table;
    cr->db = p.db;
    return cr;
}

/// @return true if cr represents a valid key column.
bool
lookupKey(QueryContext& context, boost::shared_ptr<ColumnRef> cr) {
    // Match cr as a column ref against the key column for a database's
    // partitioning strategy.
    if((!cr) || !context.metadata) { return false; }
    std::string keyColumn = context.metadata->getKeyColumn(cr->db, cr->table);
    return (!cr->column.empty()) && (keyColumn == cr->column);
}

PassTerm::Ptr newPass(std::string const& s) {
    PassTerm::Ptr p(new PassTerm);
    p->_text = s;
    return p;
}
template <typename C>
PassListTerm::Ptr newPassList(C& c) {
    PassListTerm::Ptr p(new PassListTerm);
    p->_terms.insert(p->_terms.begin(), c.begin(), c.end());
    return p;
}

InPredicate::Ptr
newInPred(std::string const& aliasTable,
          std::string const& keyColumn,
          std::vector<std::string> const& params) {
    InPredicate::Ptr p(new InPredicate());
    boost::shared_ptr<ColumnRef> cr(new ColumnRef("", aliasTable, keyColumn));
    p->value = ValueExpr::newSimple(ValueFactor::newColumnRefFactor(cr));

    typedef std::vector<std::string>::const_iterator Iter;
    for(Iter i=params.begin(), e=params.end(); i != e; ++i) {
        ValueExprPtr vep;
        vep = ValueExpr::newSimple(ValueFactor::newConstFactor(*i));
        p->cands.push_back(vep);
    }
    return p;
}

template <typename C>
FuncExpr::Ptr newFuncExpr(char const fName[],
                               std::string const& tableAlias,
                               StringPair const& chunkColumns,
                               C& c) {
    typedef boost::shared_ptr<ColumnRef> CrPtr;
    FuncExpr::Ptr fe(new FuncExpr);
    fe->name = UDF_PREFIX + fName;
    fe->params.push_back(ValueExpr::newSimple(
                             ValueFactor::newColumnRefFactor(
                                 CrPtr(new ColumnRef("", tableAlias,
                                                     chunkColumns.first)))
                             ));
    fe->params.push_back(ValueExpr::newSimple(
                             ValueFactor::newColumnRefFactor(
                                 CrPtr(new ColumnRef("", tableAlias,
                                                     chunkColumns.second)))
                             ));

    typename C::const_iterator i;
    for(i = c.begin(); i != c.end(); ++i) {
        fe->params.push_back(ValueExpr::newSimple(ValueFactor::newConstFactor(*i)));
    }
    return fe;
}


struct RestrictorEntry {
    RestrictorEntry(std::string const& alias_,
                 StringPair const& chunkColumns_,
                 std::string const& keyColumn_)
        : alias(alias_),
          chunkColumns(chunkColumns_),
          keyColumn(keyColumn_)
        {}
    std::string alias;
    StringPair chunkColumns;
    std::string keyColumn;
};
typedef std::deque<RestrictorEntry> RestrictorEntries;
class getTable {
public:

    explicit getTable(MetadataCache& metadata, RestrictorEntries& entries)
        : _metadata(metadata),
          _entries(entries) {}
    void operator()(TableRefN::Ptr t) {
        if(!t) {
            throw std::invalid_argument("NULL TableRefN::Ptr");
        }
        std::string const& db = t->getDb();
        std::string const& table = t->getTable();

        // Is table chunked?
        if(!_metadata.checkIfTableIsChunked(db, table)) {
            return; // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t->getAlias();
        if(alias.empty()) {
            // For now, only accept aliased tablerefs (should have
            // been done earlier)
            throw std::logic_error("Unexpected unaliased table reference");
        }
        std::vector<std::string> pCols = _metadata.getPartitionCols(db, table);
        RestrictorEntry se(alias,
                        StringPair(pCols[0], pCols[1]),
                        pCols[2]);
        _entries.push_back(se);
    }
    MetadataCache& _metadata;
    RestrictorEntries& _entries;
};
////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin declaration
////////////////////////////////////////////////////////////////////////
/// QservRestrictorPlugin replaces a qserv restrictor spec with directives
/// that can be executed on a qserv mysqld. This plugin should be
/// execute after aliases for tables have been generates, so that the
/// new restrictor function clauses/phrases can use the aliases.
class QservRestrictorPlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<QservRestrictorPlugin> Ptr;
    class Restriction;

    virtual ~QservRestrictorPlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(SelectStmt& stmt, QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, QueryContext& context);

private:
    BoolTerm::Ptr _makeCondition(boost::shared_ptr<QsRestrictor> const restr,
                                 RestrictorEntry const& restrictorEntry);
    boost::shared_ptr<QsRestrictor::List> _getKeyPreds(QueryContext& context, AndTerm::Ptr p);

    QsRestrictor::Ptr _newKeyRestrictor(QueryContext& context,
                                        boost::shared_ptr<ColumnRef> cr,
                                        ValueExprList& vList);
    QsRestrictor::Ptr _newKeyRestrictor(QueryContext& context,
                                        boost::shared_ptr<CompPredicate> cp);
    QsRestrictor::Ptr _convertObjectId(QueryContext& context,
                                       QsRestrictor const& original);
};

////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin::Restriction
// Generates WHERE clause terms from restriction specs. Borrowed from
// older parsing framework.
////////////////////////////////////////////////////////////////////////
class QservRestrictorPlugin::Restriction {
public:
    Restriction(QsRestrictor const& r)
        : _name(r._name) {
        _setGenerator(r);
    }
    BoolFactor::Ptr generate(RestrictorEntry const& e) {
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
        virtual BoolFactor::Ptr operator()(RestrictorEntry const& e) = 0;
    };
private:
    class ObjectIdGenerator : public Generator {
    public:
        ObjectIdGenerator(QsRestrictor::StringList const& params_)
            : params(params_.begin(), params_.end()) {
        }

        virtual BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            BoolFactor::Ptr newFactor(new BoolFactor);
            BfTerm::PtrList& terms = newFactor->_terms;
            terms.push_back(newInPred(e.alias, e.keyColumn, params));
            return newFactor;
        }
        std::vector<std::string> params;
    };

    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      QsRestrictor::StringList const& params_)
            :  fName(fName_), paramCount(paramCount_), params(params_) {
            if(paramCount_ == USE_STRING) {
                // Convert param list to one quoted string.
                // This bundles a variable-sized list into a single
                // parameter to work with the MySQL UDF facility.
            }
        }

        virtual BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            BoolFactor::Ptr newFactor(new BoolFactor);
            BfTerm::PtrList& terms = newFactor->_terms;
            CompPredicate::Ptr cp(new CompPredicate());
            boost::shared_ptr<FuncExpr> fe = newFuncExpr(fName,
                                                         e.alias,
                                                         e.chunkColumns,
                                                         params);
            cp->left = ValueExpr::newSimple(ValueFactor::newFuncFactor(fe));
            cp->op = SqlSQL2TokenTypes::EQUALS_OP;
            cp->right = ValueExpr::newSimple(ValueFactor::newConstFactor("1"));
            terms.push_back(cp);
            return newFactor;
        }
        char const* const fName;
        int const paramCount;
        QsRestrictor::StringList const& params;
        static const int USE_STRING = -999;
    };

    void _setGenerator(QsRestrictor const& r) {
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
            throw std::runtime_error("Unmatched restriction spec: " + _name);
        }
    }
    std::string _name;
    std::vector<double> _params;
    boost::shared_ptr<Generator> _generator;
};


////////////////////////////////////////////////////////////////////////
// QservRestrictorPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class QservRestrictorPluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<QservRestrictorPluginFactory> Ptr;
    QservRestrictorPluginFactory() {}
    virtual ~QservRestrictorPluginFactory() {}

    virtual std::string getName() const { return "QservRestrictor"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new QservRestrictorPlugin());
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
QservRestrictorPlugin::applyLogical(SelectStmt& stmt, QueryContext& context) {
    // Idea: For each of the qserv restrictors in the WHERE clause,
    // rewrite in the context of whatever chunked tables exist in the
    // FROM list.

    // First, get a list of the chunked tables.
    FromList& fList = stmt.getFromList();
    TableRefnList& tList = fList.getTableRefnList();
    RestrictorEntries entries;
    if(!context.metadata) {
        throw std::logic_error("Missing metadata in context");
    }
    getTable gt(*context.metadata, entries);
    std::for_each(tList.begin(), tList.end(), gt);

    if(!stmt.hasWhereClause()) { return; }

    // Prepare to patch the WHERE clause
    WhereClause& wc = stmt.getWhereClause();

    boost::shared_ptr<QsRestrictor::List const> rListP = wc.getRestrs();
    AndTerm::Ptr originalAnd(wc.getRootAndTerm());
    boost::shared_ptr<QsRestrictor::List> keyPreds;
    keyPreds = _getKeyPreds(context, originalAnd);
    AndTerm::Ptr newTerm;
    // Now handle the explicit restrictors
    if(rListP && !rListP->empty()) {
        // spatial restrictions
        QsRestrictor::List const& rList = *rListP;
        context.restrictors.reset(new QueryContext::RestrList);
        newTerm.reset(new AndTerm);

        // Now, for each of the qserv restrictors:
        for(QsRestrictor::List::const_iterator i=rList.begin();
            i != rList.end(); ++i) {
            // for each restrictor entry
            // generate a restrictor condition.
            for(RestrictorEntries::const_iterator j = entries.begin();
                j != entries.end(); ++j) {
                newTerm->_terms.push_back(_makeCondition(*i, *j));
            }
            if((**i)._name == "qserv_objectId") {
                // Convert to secIndex restrictor
                QsRestrictor::Ptr p = _convertObjectId(context, **i);
                context.restrictors->push_back(p);
            } else {
                // Save restrictor in QueryContext.
                context.restrictors->push_back(*i);
            }
        }
    }
    wc.resetRestrs();
    // Merge in the implicit restrictors
    if(keyPreds) {
        if(!context.restrictors) {
            context.restrictors = keyPreds;
        } else {
        context.restrictors->insert(context.restrictors->end(),
                                    keyPreds->begin(), keyPreds->end());
        }
    }
    if(context.restrictors && context.restrictors->empty()) {
        context.restrictors.reset();
    }
    if(newTerm) { wc.prependAndTerm(newTerm); }
}

void
QservRestrictorPlugin::applyPhysical(QueryPlugin::Plan& p, QueryContext& context) {
    // Probably nothing is needed here...
}

BoolTerm::Ptr
QservRestrictorPlugin::_makeCondition(boost::shared_ptr<QsRestrictor> const restr,
                                      RestrictorEntry const& restrictorEntry) {
    Restriction r(*restr);
    return r.generate(restrictorEntry);
}

inline void
addPred(boost::shared_ptr<QsRestrictor::List>& preds, QsRestrictor::Ptr p) {
    if(p) {
        if(!preds) {
            preds.reset(new QsRestrictor::List());
        }
        preds->push_back(p);
    }
}

boost::shared_ptr<QsRestrictor::List>
QservRestrictorPlugin::_getKeyPreds(QueryContext& context, AndTerm::Ptr p) {
    typedef BoolTerm::PtrList::iterator TermIter;
    typedef BfTerm::PtrList::iterator BfIter;
    boost::shared_ptr<QsRestrictor::List> keyPreds;

    if(!p) return keyPreds;

    for(TermIter i = p->iterBegin(); i != p->iterEnd(); ++i) {
        BoolFactor* factor = dynamic_cast<BoolFactor*>(i->get());
        if(!factor) continue;
        for(BfIter b = factor->_terms.begin();
            b != factor->_terms.end();
            ++b) {
            InPredicate::Ptr ip = boost::dynamic_pointer_cast<InPredicate>(*b);
            if(ip) {
                boost::shared_ptr<ColumnRef> cr = resolveAsColumnRef(context,
                                                                     ip->value);
                if(cr && lookupKey(context, cr)) {
                    QsRestrictor::Ptr p = _newKeyRestrictor(context,
                                                            cr,
                                                            ip->cands);
                    addPred(keyPreds, p);
                }
            } else {
                CompPredicate::Ptr cp = boost::dynamic_pointer_cast<CompPredicate>(*b);
                if(cp) {
                    QsRestrictor::Ptr p = _newKeyRestrictor(context, cp);
                    addPred(keyPreds, p);
                }
            }
        }
    }
    return keyPreds;
}


inline bool isValidLiteral(ValueExprPtr p) {
    return p && !p->copyAsLiteral().empty();
}

struct validateLiteral {
    validateLiteral(bool& isValid_) : isValid(isValid_) {}
    inline void operator()(ValueExprPtr p) {
        isValid = isValid && isValidLiteral(p);
    }
    bool& isValid;
};

struct extractLiteral {
    inline std::string operator()(ValueExprPtr p) {
        return p->copyAsLiteral();
    }
};
/// @return a new QsRestrictor from the column ref and the set of
/// specified values or NULL if one of the values is a non-literal.
QsRestrictor::Ptr
QservRestrictorPlugin::_newKeyRestrictor(QueryContext& context,
                                     boost::shared_ptr<ColumnRef> cr,
                                     ValueExprList& vList) {
    // Extract the literals, bailing out if we see a non-literal
    bool isValid = true;
    std::for_each(vList.begin(), vList.end(), validateLiteral(isValid));
    if(!isValid) {
        return QsRestrictor::Ptr();
    }

    // Build the QsRestrictor
    QsRestrictor::Ptr p(new QsRestrictor());
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
QsRestrictor::Ptr
QservRestrictorPlugin::_newKeyRestrictor(QueryContext& context,
                                         boost::shared_ptr<CompPredicate> cp) {
    QsRestrictor::Ptr p;
    boost::shared_ptr<ColumnRef> key = resolveAsColumnRef(context,
                                                          cp->left);
    int op = cp->op;
    ValueExprPtr literalValue = cp->right;
    // Find the key column ref: Is it on the rhs or lhs?
    if(key && lookupKey(context, key)) {
        // go on.
    } else {
        key = resolveAsColumnRef(context, cp->right);
        if(key && lookupKey(context, key)) {
            op = CompPredicate::reverseOp(op);
            literalValue = cp->left;
        } else {
            return p; // No key column ref. Leave it alone.
        }
    }
    // Make sure the expected literal is a literal
    bool isValid = true;
    validateLiteral vl(isValid);
    vl(literalValue);
    if(!isValid) { return p; } // No key. Leave alone.
    std::list<boost::shared_ptr<ValueExpr> > cands;
    cands.push_back(literalValue);
    return _newKeyRestrictor(context, key, cands);
}
QsRestrictor::Ptr
QservRestrictorPlugin::_convertObjectId(QueryContext& context,
                                    QsRestrictor const& original) {
    // Build the QsRestrictor
    QsRestrictor::Ptr p(new QsRestrictor());
    p->_name = "sIndex";
    // sIndex has paramers as follows:
    // db, table, column, val1, val2, ...
    p->_params.push_back(context.dominantDb);
    p->_params.push_back(context.anonymousTable);
    std::string keyColumn = context.metadata->getKeyColumn(context.dominantDb,
                                                           context.anonymousTable);
    p->_params.push_back(keyColumn);
    std::copy(original._params.begin(), original._params.end(),
              std::back_inserter(p->_params));
    return p;
}

}}} // namespace lsst::qserv::master
