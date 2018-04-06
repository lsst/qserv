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
/**
  * @file
  *
  * @brief QservRestrictorPlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qana/QservRestrictorPlugin.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.QservRestrictorPlugin");

enum RestrictorType { SECONDARY_INDEX_IN =1, SECONDARY_INDEX_BETWEEN };

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qana {

typedef std::pair<std::string, std::string> StringPair;

/// Returns a list of column references.
/// This class will usually check the list if any can be used
/// to restrict the query to a subset of worker nodes via the secondary index.
/// If there is ambiguity in the possible table that a column belongs to,
/// the SQL statement should cause an error when tried on the local database,
/// so that check can be skipped.
///
query::ColumnRef::Vector resolveAsColumnRef(query::QueryContext& context, query::ValueExprPtr vexpr) {
    query::ColumnRef::Vector columnRefs;
    query::ColumnRef::Ptr cr = vexpr->copyAsColumnRef();
    if (!cr) {
        return columnRefs;
    }
    std::string column = cr->column;
    query::DbTableSet set = context.resolve(cr);
    for (auto const& dbTblPair : set) {
        columnRefs.push_back(std::make_shared<query::ColumnRef>(dbTblPair.db, dbTblPair.table, column));
    }
    return columnRefs;
}


/// @return true if cr represents a valid secondary index column.
bool
lookupSecIndex(query::QueryContext& context,
               std::shared_ptr<query::ColumnRef> cr) {
    // Match cr as a column ref against the secondary index column for a
    // database's partitioning strategy.
    if ((!cr) || !context.css) { return false; }
    if (!context.css->containsDb(cr->db)
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

/**  Create a QsRestrictor from the column ref and the set of specified values or NULL if one of the values is a non-literal.
 *
 *   @param restrictorType: The type of restrictor, only secondary index restrictors are handled
 *   @param context:        Context, used to get database schema informations
 *   @param cr:             Represent the column on which secondary index will be queried
 *   @return:               A Qserv restrictor or NULL if at least one element in values is a non-literal.
 */
query::QsRestrictor::Ptr newRestrictor(
    RestrictorType restrictorType,
    query::QueryContext const& context,
    std::shared_ptr<query::ColumnRef> cr,
    query::ValueExprPtrVector const& values)
{
    // Extract the literals, bailing out if we see a non-literal
    bool isValid = true;
    std::for_each(values.begin(), values.end(), validateLiteral(isValid));
    if (!isValid) {
        return query::QsRestrictor::Ptr();
    }

    // Build the QsRestrictor
    query::QsRestrictor::Ptr restrictor = std::make_shared<query::QsRestrictor>();
    if (restrictorType==SECONDARY_INDEX_IN) {
        restrictor->_name = "sIndex";
    }
    else if (restrictorType==SECONDARY_INDEX_BETWEEN) {
        restrictor->_name = "sIndexBetween";
    }
    // sIndex and sIndexBetween have parameters as follows:
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
                LOGS(_log, LOG_LVL_ERROR, "dirTable missing, but dirDb is set inconsistently for "
                     << cr->db << "." << cr->table);
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
                LOGS(_log, LOG_LVL_ERROR, "dirCol missing for " << dirDb << "." << dirTable);
                return query::QsRestrictor::Ptr();
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "Restrictor dirDb " << dirDb << ", dirTable " << dirTable
             << ", dirCol " << dirCol << " as sIndex for " << cr->db << "." << cr->table
             << "." << cr->column);
        restrictor->_params.push_back(dirDb);
        restrictor->_params.push_back(dirTable);
        restrictor->_params.push_back(dirCol);
    } else {
        LOGS_DEBUG("Restrictor " << cr->db << "." << cr->table <<  "." << cr->column
                   << " as sIndex");
        restrictor->_params.push_back(cr->db);
        restrictor->_params.push_back(cr->table);
        restrictor->_params.push_back(cr->column);
    }
    std::transform(values.begin(), values.end(),
                   std::back_inserter(restrictor->_params), extractLiteral());
    return restrictor;
}

/**  Create QSRestrictors which will use secondary index
 *
 *   @param context:  Context used to analyze SQL query, allow to compute
 *                    column names and find if they are in secondary index.
 *   @param andTerm:  Intermediatre representation of a subset of a SQL WHERE clause
 *
 *   @return:         Qserv restrictors list
 */
query::QsRestrictor::PtrVector getSecIndexRestrictors(query::QueryContext& context,
                                                      query::AndTerm::Ptr andTerm) {
    query::QsRestrictor::PtrVector result;
    if (not andTerm) return result;

    for (auto term : andTerm->_terms) {
        query::BoolFactor* factor = dynamic_cast<query::BoolFactor*>(term.get());
        if (!factor) continue;
        for (auto factorTerm : factor->_terms) {

            query::ColumnRef::Vector columnRefs;
            query::QsRestrictor::Ptr restrictor;

            // Remark: computation below could be placed in a virtual *Predicate::newRestrictor() method
            // but this is not obvious because it would move restrictor-related code outside of
            // the current plugin.
            // IN predicate
            LOGS(_log, LOG_LVL_TRACE, "Check for SECONDARY_INDEX_IN restrictor");
            if (auto const inPredicate = std::dynamic_pointer_cast<query::InPredicate>(factorTerm)) {
                columnRefs = resolveAsColumnRef(context, inPredicate->value);
                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        restrictor = newRestrictor(
                                     SECONDARY_INDEX_IN, context, column_ref, inPredicate->cands);
                        LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_IN restrictor: " << *restrictor);
                        break; // Only want one per column.
                    }
                }
            } else if (auto const compPredicate =
                       std::dynamic_pointer_cast<query::CompPredicate>(factorTerm)) {
                // '=' predicate
                query::ValueExprPtr literalValue;

                // If the left side doesn't match any columns, check the right side.
                columnRefs = resolveAsColumnRef(context, compPredicate->left);
                if (columnRefs.empty()) {
                    columnRefs = resolveAsColumnRef(context, compPredicate->right);
                    literalValue = compPredicate->left;
                } else {
                    literalValue = compPredicate->right;
                }

                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        query::ValueExprPtrVector cands(1, literalValue);
                        restrictor = newRestrictor(SECONDARY_INDEX_IN, context, column_ref , cands);
                        if (restrictor) {
                            LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_IN restrictor: "
                                    << *restrictor << " for '=' predicate");
                            break; // Only want one per column.
                        }
                    }
                }
            } else if (auto const betweenPredicate =
                       std::dynamic_pointer_cast<query::BetweenPredicate>(factorTerm)) {
                // BETWEEN predicate
                LOGS(_log, LOG_LVL_TRACE, "Check for SECONDARY_INDEX_BETWEEN restrictor");
                columnRefs = resolveAsColumnRef(context, betweenPredicate->value);
                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        query::ValueExprPtrVector cands;
                        cands.push_back(betweenPredicate->minValue);
                        cands.push_back(betweenPredicate->maxValue);
                        restrictor = newRestrictor(
                            RestrictorType::SECONDARY_INDEX_BETWEEN, context, column_ref, cands);
                        if (restrictor) {
                            LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_BETWEEN restrictor: "
                                                      << *restrictor);
                            break; // Only want one per column.
                        }
                    }
                }
            }

            if (restrictor) {
                result.push_back(restrictor);
            }
        }
    }
    return result;
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
    for (Iter i=params.begin(), e=params.end(); i != e; ++i) {
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
    fe->setName(UDF_PREFIX + fName);
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                  std::make_shared<query::ColumnRef>(
                          "", tableAlias, chunkColumns.first))));
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                  std::make_shared<query::ColumnRef>(
                          "", tableAlias, chunkColumns.second))));

    typename C::const_iterator i;
    for (i = c.begin(); i != c.end(); ++i) {
        fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(*i)));
    }
    return fe;
}


typedef std::deque<RestrictorEntry> RestrictorEntries;

class getTable : public query::TableRef::Func {
public:

    getTable(css::CssAccess& css, RestrictorEntries& entries)
        : _css(css),
          _entries(entries) {}

    void operator()(query::TableRef::Ptr t) {
        // FIXME: Modify so we can use TableRef::apply()
        if (!t) {
            throw AnalysisBug("NULL TableRefN::Ptr");
        }
        (*this)(*t);
    }
    virtual void operator()(query::TableRef& t) {
        std::string const& db = t.getDb();
        std::string const& table = t.getTable();

        if (db.empty()
           || !_css.containsDb(db)
           || !_css.containsTable(db, table)) {
            throw AnalysisError("Invalid db/table:" + db + "." + table);
        }
        css::PartTableParams const& partParam = _css.getPartTableParams(db, table);
        // Is table chunked?
        if (!partParam.isChunked()) {
            return; // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t.getAlias();
        if (alias.empty()) {
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
        for (Iter i=joinRefs.begin(), e=joinRefs.end(); i != e; ++i) {
            (*this)((**i).getRight());
        }
    }
    css::CssAccess& _css;
    RestrictorEntries& _entries;
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
    //     if (_generator.get()) {
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
    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      StringVector const& params_)
            :  fName(fName_), paramCount(paramCount_), params(params_) {
            if (paramCount_ == USE_STRING) {
                // Convert param list to one quoted string.
                // This bundles a variable-sized list into a single
                // parameter to work with the MySQL UDF facility.
            }
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor =
                    std::make_shared<query::BoolFactor>();
            query::BoolFactorTerm::PtrVector& terms = newFactor->_terms;
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
        if (r._name == "qserv_areaspec_box") {
            _generator = std::make_shared<AreaGenerator>("s2PtInBox",
                                                         4,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_circle") {
            _generator = std::make_shared<AreaGenerator>("s2PtInCircle",
                                                         3,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_ellipse") {
            _generator = std::make_shared<AreaGenerator>("s2PtInEllipse",
                                                         5,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_poly") {
            const int use_string = AreaGenerator::USE_STRING;
            _generator = std::make_shared<AreaGenerator>("s2PtInCPoly",
                                                         use_string,
                                                         r._params
                                                         );
        } else {
            throw AnalysisBug("Unmatched restriction spec: " + _name);
        }
    }
    std::string _name;
    std::vector<double> _params;
    std::shared_ptr<Generator> _generator;
};


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
    if (!context.css) {
        throw AnalysisBug("Missing metadata in context");
    }
    getTable gt(*context.css, entries);
    std::for_each(tList.begin(), tList.end(), gt);

    if (!stmt.hasWhereClause()) { return; }

    // Prepare to patch the WHERE clause
    query::WhereClause& whereClause = stmt.getWhereClause();

    query::AndTerm::Ptr originalAnd(whereClause.getRootAndTerm());
    query::QueryContext::RestrList ctxRestrictors;

    // Now handle the explicit restrictors
    auto whereClauseRestrictors = whereClause.getRestrs();
    if (whereClauseRestrictors && not whereClauseRestrictors->empty()) {

        // At least one table should exist in the restrictor entries
        if (entries.empty()) {
            throw AnalysisError("Spatial restrictor w/o partitioned table");
        }

        auto newTerm = std::make_shared<query::AndTerm>();
        // spatial restrictions
        // Now, for each of the qserv restrictors:
        for (auto const& restrictor : *whereClauseRestrictors) {
            // for each restrictor entry
            // generate a restrictor condition.
            for (auto const& entry : entries) {
                newTerm->_terms.push_back(_makeCondition(restrictor, entry));
            }
            // Save restrictor in QueryContext.
            ctxRestrictors.push_back(restrictor);
        }

        whereClause.prependAndTerm(newTerm);
    }
    whereClause.resetRestrs();

    // Merge in the implicit (i.e. secondary index) restrictors
    query::QsRestrictor::PtrVector const& secIndexPreds = getSecIndexRestrictors(context, originalAnd);
    ctxRestrictors.insert(ctxRestrictors.end(), secIndexPreds.begin(), secIndexPreds.end());

    if (not ctxRestrictors.empty()) {
        context.restrictors = std::make_shared<query::QueryContext::RestrList>(ctxRestrictors);
    }
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

}}} // namespace lsst::qserv::qana
