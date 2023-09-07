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
#include "query/AndTerm.h"
#include "query/AreaRestrictor.h"
#include "query/BoolFactor.h"
#include "query/BetweenPredicate.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/InPredicate.h"
#include "query/JoinRef.h"
#include "query/PassTerm.h"
#include "query/PassListTerm.h"
#include "query/QueryContext.h"
#include "query/SecIdxRestrictor.h"
#include "query/SelectStmt.h"
#include "query/ValueFactor.h"
#include "query/ValueExpr.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"

namespace {

using namespace lsst::qserv;

LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.QservRestrictorPlugin");

/// RestrictorEntry is a class to contain information about chunked tables.
struct RestrictorEntry {
    RestrictorEntry(std::string const& alias_, StringPair const& chunkColumns_,
                    std::string const& secIndexColumn_)
            : alias(alias_), chunkColumns(chunkColumns_), secIndexColumn(secIndexColumn_) {}
    std::string const alias;  //< The alias of the chunked table.
    StringPair const chunkColumns;
    std::string const secIndexColumn;
};

typedef std::pair<std::string, std::string> StringPair;
typedef std::deque<RestrictorEntry> RestrictoryEntryList;

class GetTable : public query::TableRef::Func {
public:
    GetTable(css::CssAccess& css, RestrictoryEntryList& chunkedTables)
            : _css(css), _chunkedTables(chunkedTables) {}

    void operator()(query::TableRef::Ptr t) {
        // FIXME: Modify so we can use TableRef::apply()
        if (!t) {
            throw qana::AnalysisBug("NULL TableRefN::Ptr");
        }
        (*this)(*t);
    }
    virtual void operator()(query::TableRef& t) {
        std::string const& db = t.getDb();
        std::string const& table = t.getTable();

        if (db.empty() || table.empty() || !_css.containsDb(db) || !_css.containsTable(db, table)) {
            throw qana::AnalysisError("Invalid db/table:" + db + "." + table);
        }
        css::PartTableParams const& partParam = _css.getPartTableParams(db, table);
        // Is table chunked?
        if (!partParam.isChunked()) {
            return;  // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t.getAlias();
        if (alias.empty()) {
            // For now, only accept aliased tablerefs (should have
            // been done earlier)
            throw qana::AnalysisBug("Unexpected unaliased table reference");
        }
        std::vector<std::string> pCols = partParam.partitionCols();
        RestrictorEntry se(alias, StringPair(pCols[0], pCols[1]), pCols[2]);
        _chunkedTables.push_back(se);
        for (auto const& joinRef : t.getJoins()) {
            (*this)(joinRef->getRight());
        }
    }
    css::CssAccess& _css;
    RestrictoryEntryList& _chunkedTables;
};

/**
 * @brief Determine if the given ValueExpr represents a func that is one of the scisql functions that
 *        starts with `scisql_s2PtIn` that represents an area restrictor.
 *
 * This is a helper function for handleScisqlRestrictors.
 *
 * @param valueExpr The ValueExpr to check.
 * @return true if the ValueExpr is a function and starts with `scisql_s2PtIn` else false.
 */
bool isScisqlAreaFunc(query::ValueExpr const& valueExpr) {
    if (not valueExpr.isFunction()) return false;
    auto&& funcExpr = valueExpr.getFunction();
    if (funcExpr->getName().find("scisql_s2Pt") != 0) return false;
    return true;
}

/**
 * @brief If there is exactly one scisql area restrictor in the top level AND of the where clause,
 *        return it.
 *
 * @param whereClause The WHERE clause to look in.
 * @return std::shared_ptr<const query::FuncExpr> The scisql area restrictor functio FuncExpr if there
 *         was exactly one, else nullptr.
 */
std::shared_ptr<const query::FuncExpr> extractSingleScisqlAreaFunc(query::WhereClause const& whereClause) {
    auto topLevelAnd = whereClause.getRootAndTerm();
    std::shared_ptr<const query::FuncExpr> scisqlFunc;
    if (nullptr == topLevelAnd) return nullptr;
    for (auto const& boolTerm : topLevelAnd->_terms) {
        auto boolFactor = std::dynamic_pointer_cast<const query::BoolFactor>(boolTerm);
        if (nullptr == boolFactor) continue;
        for (auto boolFactorTerm : boolFactor->_terms) {
            auto compPredicate = std::dynamic_pointer_cast<const query::CompPredicate>(boolFactorTerm);
            if (nullptr == compPredicate) continue;
            if (compPredicate->op != query::CompPredicate::EQUALS_OP) continue;
            for (auto const& valueExpr : {compPredicate->left, compPredicate->right}) {
                if (isScisqlAreaFunc(*valueExpr)) {
                    if (scisqlFunc != nullptr) {
                        return nullptr;
                    }
                    scisqlFunc = valueExpr->getFunction();
                }
            }
        }
    }
    return scisqlFunc;
}

std::shared_ptr<query::AreaRestrictor> makeAreaRestrictor(query::FuncExpr const& scisqlFunc) {
    std::vector<std::string> parameters;
    int counter(0);
    for (auto const& valueExpr : scisqlFunc.getParams()) {
        if (counter++ < 2) {
            // The first 2 parameters are the ra and decl columns to test; these get thrown away.
            continue;
        }
        if (valueExpr->isConstVal()) {
            parameters.push_back(valueExpr->getConstVal());
        } else {
            // If any parameter in the scisql restrictor function is not a const value then we can't use it
            // (for example, we don't support math functions in the area restrictor.)
            // Give up & carry on.
            return nullptr;
        }
    }
    try {
        auto&& name = scisqlFunc.getName();
        if (name == "scisql_s2PtInBox") {
            return std::make_shared<query::AreaRestrictorBox>(std::move(parameters));
        } else if (name == "scisql_s2PtInCircle") {
            return std::make_shared<query::AreaRestrictorCircle>(std::move(parameters));
        } else if (name == "scisql_s2PtInEllipse") {
            return std::make_shared<query::AreaRestrictorEllipse>(std::move(parameters));
        } else if (name == "scisql_s2PtInCPoly") {
            return std::make_shared<query::AreaRestrictorPoly>(std::move(parameters));
        }
    } catch (std::logic_error const& err) {
        throw std::runtime_error("Wrong number of arguments for " + scisqlFunc.getName());
    }
    return nullptr;
}

/**
 * @brief Add scisql restrictors for each AreaRestrictor.
 *
 * This handles the case where a qserv areaspec function was passed into the WHERE clause by the user,
 * it adds scisql restrictor functions corresponding to the qserv area restrictor that is applied as a
 * result of the areaspec function.
 *
 * @param restrictors The qserv area restrictors to add scisql area restrictors for.
 * @param whereClause The query's WHERE clause, to which scisql area restrictors will be added.
 * @param fromList The query's FROM list, it is consulted to find the chunked tables.
 * @param context The query context.
 */
void addScisqlRestrictors(std::vector<std::shared_ptr<query::AreaRestrictor>> const& areaRestrictors,
                          query::FromList const& fromList, query::WhereClause& whereClause,
                          query::QueryContext& context) {
    if (areaRestrictors.empty()) return;

    auto const& tableList = fromList.getTableRefList();
    RestrictoryEntryList chunkedTables;
    GetTable gt(*context.css, chunkedTables);
    std::for_each(tableList.begin(), tableList.end(), gt);
    // chunkedTables is now populated with a RestrictorEntry for each table in the FROM list that is chunked.
    if (chunkedTables.empty()) {
        throw qana::AnalysisError("Spatial restrictor without partitioned table.");
    }

    auto newTerm = std::make_shared<query::AndTerm>();
    // Add scisql spatial restrictions: for each of the qserv restrictors, generate a scisql restrictor
    // condition for each chunked table.
    for (auto const& areaRestrictor : areaRestrictors) {
        for (auto const& chunkedTable : chunkedTables) {
            newTerm->_terms.push_back(
                    areaRestrictor->asSciSqlFactor(chunkedTable.alias, chunkedTable.chunkColumns));
        }
    }
    LOGS(_log, LOG_LVL_TRACE,
         "for restrictors: " << util::printable(areaRestrictors) << " adding: " << newTerm);
    whereClause.prependAndTerm(newTerm);
}

/**
 * @brief Find out if the given ColumnRef represents a valid secondary index column.
 */
bool isSecIndexCol(query::QueryContext const& context, std::shared_ptr<query::ColumnRef> cr) {
    // Match cr as a column ref against the secondary index column for a
    // database's partitioning strategy.
    if (nullptr == cr || nullptr == context.css) return false;
    if (not context.css->containsDb(cr->getDb()) ||
        not context.css->containsTable(cr->getDb(), cr->getTable())) {
        throw qana::AnalysisError("Invalid db/table:" + cr->getDb() + "." + cr->getTable());
    }
    if (cr->getColumn().empty()) {
        return false;
    }
    std::vector<std::string> sics =
            context.css->getPartTableParams(cr->getDb(), cr->getTable()).secIndexColNames();
    std::string const& column = cr->getColumn();
    return std::find_if(sics.begin(), sics.end(),
                        [&column](auto const& str) { return boost::iequals(column, str); }) != sics.end();
}

query::ColumnRef::Ptr getCorrespondingDirectorColumn(query::QueryContext const& context,
                                                     query::ColumnRef::Ptr const& columnRef) {
    if (nullptr == columnRef) return nullptr;
    auto const partitionTableParams =
            context.css->getPartTableParams(columnRef->getDb(), columnRef->getTable());
    // Get the director column name
    std::string dirCol = partitionTableParams.dirColName;
    if (columnRef->getColumn() == dirCol) {
        // columnRef may be a column in a child table, in which case we must figure
        // out the corresponding column in the child's director to properly
        // generate a secondary index restrictor.
        std::string dirDb = partitionTableParams.dirDb;
        std::string dirTable = partitionTableParams.dirTable;
        if (dirTable.empty()) {
            dirTable = columnRef->getTable();
            if (!dirDb.empty() && dirDb != columnRef->getDb()) {
                LOGS(_log, LOG_LVL_ERROR,
                     "dirTable missing, but dirDb is set inconsistently for " << columnRef->getDb() << "."
                                                                              << columnRef->getTable());
                return nullptr;
            }
            dirDb = columnRef->getDb();
        } else if (dirDb.empty()) {
            dirDb = columnRef->getDb();
        }
        if (dirDb != columnRef->getDb() || dirTable != columnRef->getTable()) {
            // Lookup the name of the director column in the director table
            dirCol = context.css->getPartTableParams(dirDb, dirTable).dirColName;
            if (dirCol.empty()) {
                LOGS(_log, LOG_LVL_ERROR, "dirCol missing for " << dirDb << "." << dirTable);
                return nullptr;
            }
        }
        LOGS(_log, LOG_LVL_TRACE,
             "Restrictor dirDb " << dirDb << ", dirTable " << dirTable << ", dirCol " << dirCol
                                 << " as sIndex for " << columnRef->getDb() << "." << columnRef->getTable()
                                 << "." << columnRef->getColumn());
        return std::make_shared<query::ColumnRef>(dirDb, dirTable, dirCol);
    }
    LOGS_DEBUG("Restrictor " << columnRef << " as sIndex.");
    return columnRef;
}

/**
 * @brief Make a Secondary Index comparison restrictor for the given 'in' predicate, if the value column
 *      of the predicate is a director column.
 *
 * @param inPredicate
 * @param context
 * @return std::shared_ptr<query::SecIdxInRestrictor> The restrictor that corresponds to the given predicate
 * if the value column is a director column, otherwise nullptr.
 */
std::shared_ptr<query::SecIdxInRestrictor> makeSecondaryIndexRestrictor(query::InPredicate const& inPredicate,
                                                                        query::QueryContext const& context) {
    if (isSecIndexCol(context, inPredicate.value->getColumnRef())) {
        auto dirCol = getCorrespondingDirectorColumn(context, inPredicate.value->getColumnRef());
        if (nullptr == dirCol) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Failed to get director column for " << inPredicate.value->getColumnRef());
            return nullptr;
        }
        return std::make_shared<query::SecIdxInRestrictor>(std::make_shared<query::InPredicate>(
                query::ValueExpr::newSimple(dirCol), inPredicate.cands, inPredicate.hasNot));
    }
    return nullptr;
}

/**
 * @brief Make a Secondary Index 'between' restrictor for the given between predicate, if one of the
 *      columns in the predicate is a director column.
 *
 * @param betweenPredicate
 * @param context
 * @return std::shared_ptr<query::SIBetweenRestr> The restrictor that corresponds to the given predicate if
 * one of the columns is a director column, otherwise nullptr.
 */
[[maybe_unused]] std::shared_ptr<query::SecIdxBetweenRestrictor> makeSecondaryIndexRestrictor(
        query::BetweenPredicate const& betweenPredicate, query::QueryContext const& context) {
    if (isSecIndexCol(context, betweenPredicate.value->getColumnRef())) {
        auto dirCol = getCorrespondingDirectorColumn(context, betweenPredicate.value->getColumnRef());
        if (nullptr == dirCol) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Failed to get director column for " << betweenPredicate.value->getColumnRef());
            return nullptr;
        }
        return std::make_shared<query::SecIdxBetweenRestrictor>(std::make_shared<query::BetweenPredicate>(
                query::ValueExpr::newSimple(dirCol), betweenPredicate.minValue, betweenPredicate.maxValue,
                betweenPredicate.hasNot));
    }
    return nullptr;
}

/**
 * @brief Make a Secondary Index comparison restrictor for the given comparison predicate, if one of the
 *      columns in the comparison predicate is a director column.
 *
 * @param compPredicate
 * @param context
 * @return std::shared_ptr<query::SICompRestr> The restrictor that corresponds to the given predicate if one
 *      of the columns is a director column, otherwise nullptr.
 */
std::shared_ptr<query::SecIdxCompRestrictor> makeSecondaryIndexRestrictor(
        query::CompPredicate const& compPredicate, query::QueryContext const& context) {
    if (compPredicate.right->isConstVal() && isSecIndexCol(context, compPredicate.left->getColumnRef())) {
        auto dirCol = getCorrespondingDirectorColumn(context, compPredicate.left->getColumnRef());
        if (nullptr == dirCol) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Failed to get director column for " << compPredicate.left->getColumnRef());
            return nullptr;
        }
        auto siCompPred = std::make_shared<query::CompPredicate>(query::ValueExpr::newSimple(dirCol),
                                                                 compPredicate.op, compPredicate.right);
        return std::make_shared<query::SecIdxCompRestrictor>(siCompPred, true);
    } else if (compPredicate.left->isConstVal() &&
               isSecIndexCol(context, compPredicate.right->getColumnRef())) {
        auto dirCol = getCorrespondingDirectorColumn(context, compPredicate.right->getColumnRef());
        if (nullptr == dirCol) {
            LOGS(_log, LOG_LVL_ERROR,
                 "Failed to get director column for " << compPredicate.right->getColumnRef());
            return nullptr;
        }
        auto siCompPred = std::make_shared<query::CompPredicate>(compPredicate.left, compPredicate.op,
                                                                 query::ValueExpr::newSimple(dirCol));
        return std::make_shared<query::SecIdxCompRestrictor>(siCompPred, false);
    }
    return nullptr;
}

/**  Create SIRestrictors which will use secondary index
 *
 *   @param context:  Context used to analyze SQL query, allow to compute
 *                    column names and find if they are in secondary index.
 *   @param andTerm:  Intermediatre representation of a subset of a SQL WHERE clause
 *
 *   @return:         Qserv restrictors list
 */
std::vector<std::shared_ptr<query::SecIdxRestrictor>> getSecIndexRestrictors(query::QueryContext& context,
                                                                             query::AndTerm::Ptr andTerm) {
    std::vector<std::shared_ptr<query::SecIdxRestrictor>> result;
    if (not andTerm) return result;

    for (auto&& term : andTerm->_terms) {
        auto factor = std::dynamic_pointer_cast<query::BoolFactor>(term);
        if (!factor) continue;
        for (auto factorTerm : factor->_terms) {
            std::shared_ptr<query::SecIdxRestrictor> restrictor;
            if (auto const inPredicate = std::dynamic_pointer_cast<query::InPredicate>(factorTerm)) {
                restrictor = makeSecondaryIndexRestrictor(*inPredicate, context);
            } else if (auto const compPredicate =
                               std::dynamic_pointer_cast<query::CompPredicate>(factorTerm)) {
                if (compPredicate->op == query::CompPredicate::EQUALS_OP) {
                    restrictor = makeSecondaryIndexRestrictor(*compPredicate, context);
                }
            }
            if (restrictor) {
                LOGS(_log, LOG_LVL_TRACE, "Add restrictor: " << *restrictor << " for " << factorTerm);
                result.push_back(restrictor);
            }
        }
    }
    return result;
}

/**
 * @brief Looks in the WHERE clause for use of columns from chunked tables where chunk restrictions can
 *        be added, and adds qserv restrictor functions if any are found.
 *
 * @param whereClause The WHERE clause of the SELECT statement.
 * @param context The context to add the restrictor functions to.
 */
void handleSecondaryIndex(query::WhereClause& whereClause, query::QueryContext& context) {
    // Merge in the implicit (i.e. secondary index) restrictors
    query::AndTerm::Ptr originalAnd(whereClause.getRootAndTerm());
    auto const& secIndexPreds = getSecIndexRestrictors(context, originalAnd);
    context.addSecIdxRestrictors(secIndexPreds);
}

}  // anonymous namespace

namespace lsst::qserv::qana {

////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin implementation
////////////////////////////////////////////////////////////////////////
void QservRestrictorPlugin::applyLogical(query::SelectStmt& stmt, query::QueryContext& context) {
    // Idea: For each of the qserv restrictors in the WHERE clause,
    // rewrite in the context of whatever chunked tables exist in the
    // FROM list.

    if (!context.css) {
        throw AnalysisBug("Missing metadata in context.");
    }

    // If there's no where clause then there's no need to do any work here.
    if (!stmt.hasWhereClause()) {
        return;
    }

    // Prepare to patch the WHERE clause
    query::WhereClause& whereClause = stmt.getWhereClause();

    if (whereClause.hasRestrs()) {
        // get where clause restrictors:
        auto restrictors = whereClause.getRestrs();
        // add restrictors to context:
        if (restrictors != nullptr) {
            context.addAreaRestrictors(*restrictors);
            whereClause.resetRestrs();
            // make scisql functions for restrictors:
            addScisqlRestrictors(*restrictors, stmt.getFromList(), whereClause, context);
        }
    } else {
        // Get scisql restrictor if there is exactly one of them
        auto scisqlFunc = extractSingleScisqlAreaFunc(whereClause);
        if (scisqlFunc != nullptr) {
            // Attempt to convirt the scisql restrictor to an AreaRestrictor. This will fail if any parameter
            // in the scisql function is NOT a const val.
            auto areaRestrictor = makeAreaRestrictor(*scisqlFunc);
            if (areaRestrictor != nullptr) {
                LOGS(_log, LOG_LVL_TRACE, "Adding restrictor: " << *areaRestrictor);
                context.addAreaRestrictors({areaRestrictor});
            }
        }
    }

    handleSecondaryIndex(whereClause, context);
}

void QservRestrictorPlugin::applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context) {
    // Probably nothing is needed here...
}

}  // namespace lsst::qserv::qana
