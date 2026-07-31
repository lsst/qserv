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
#include <cmath>
#include <deque>
#include <memory>
#include <string>

// Third-party headers
#include "boost/algorithm/string/predicate.hpp"  // for iequals

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
 * @brief Determine if all area parameters of a scisql area function (everything after the leading longitude
 *        and latitude) are constant values. If any parameters are non-constants then we have a spatial join
 *        instead (see qana::RelationGraph).
 *
 * @param scisqlFunc The scisql area function to check.
 * @return if true if all area parameters are constant, false otherwise.
 */
bool areaParamsAreConst(query::FuncExpr const& scisqlFunc) {
    auto const& params = scisqlFunc.getParams();
    if (params.size() <= 2) return false;
    for (auto it = params.begin() + 2; it != params.end(); ++it) {
        if ((*it)->isConstVal() == false) {
            return false;
        }
    }
    return true;
}

/**
 * @brief An area restrictor can only be applied if its lon/lat are the partitioning columns for the table.
 *
 * @param lonColumnRef longitude column passed to the area function
 * @param latColumnRef latitude column passed to the area function
 * @param context query context used to look up the partitioning columns in CSS.
 *
 * @return true if lonColumnRef/latColumnRef are the partitioning columns of a chunked table, false otherwise
 */
bool isAdmissibleAreaRestrictor(query::ColumnRef const& lonColumnRef, query::ColumnRef const& latColumnRef,
                                query::QueryContext const& context) {
    // Check to make sure these columns come from the same db/table
    std::string const& db = lonColumnRef.getDb();
    std::string const& table = lonColumnRef.getTable();
    if (db.empty() || table.empty() || db != latColumnRef.getDb() || table != latColumnRef.getTable() ||
        lonColumnRef.getTableAlias() != latColumnRef.getTableAlias()) {
        return false;
    }

    if (!context.css->containsDb(db) || !context.css->containsTable(db, table)) {
        return false;
    }

    css::PartTableParams const partParam = context.css->getPartTableParams(db, table);
    // check the table is chunked and that column args passed in are the partitioning columns
    return partParam.isChunked() && !partParam.lonColName.empty() && !partParam.latColName.empty() &&
           boost::iequals(lonColumnRef.getColumn(), partParam.lonColName) &&
           boost::iequals(latColumnRef.getColumn(), partParam.latColName);
}

std::shared_ptr<query::AreaRestrictor> makeAreaRestrictor(query::FuncExpr const& scisqlFunc,
                                                          query::QueryContext const& context) {
    auto const& funcParams = scisqlFunc.getParams();
    if (funcParams.size() < 2) return nullptr;

    auto const lonColumnRef = funcParams[0]->getColumnRef();
    auto const latColumnRef = funcParams[1]->getColumnRef();
    if (!lonColumnRef || !latColumnRef) {
        // not plain column refs; can't be used as an area restrictor
        return nullptr;
    }

    // Ensure the lon/lat passed to this function are the columns the table is partitioned by
    if (!isAdmissibleAreaRestrictor(*lonColumnRef, *latColumnRef, context)) {
        LOGS(_log, LOG_LVL_DEBUG,
             "Not using " << scisqlFunc.getName() << " as an area restrictor; (" << lonColumnRef->getColumn()
                          << ", " << latColumnRef->getColumn() << ") are not the partitioning columns of "
                          << lonColumnRef->getDb() << "." << lonColumnRef->getTable());
        return nullptr;
    }

    std::vector<std::string> parameters;
    int counter(0);
    for (auto const& valueExpr : scisqlFunc.getParams()) {
        if (counter++ < 2) {
            // The first 2 parameters are the ra and decl columns to test; these get thrown away but were
            // tested for validity above.
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
 * @brief Convert an angular separation to area restrictor, IF exactly one of the two coordinate pairs is
 *        constant. For instance, WHERE scisql_angSep(ra, dec, 55.5, -30.2) < 0.05 is
 *        equivalent to WHERE scisql_s2PtInCircle(ra, dec, 55.5, -30.2, 0.05) = 1.
 *
 * @param compPredicate The comparison predicate to convert.
 * @param context query context used to check admissibility as an area restrictor
 * @return std::shared_ptr<query::AreaRestrictor> Resulting area restrictor, nullptr otherwise
 */
std::shared_ptr<query::AreaRestrictor> makeAreaRestrictorFromAngSep(query::CompPredicate const& compPredicate,
                                                                    query::QueryContext const& context) {
    // If we have scisql_angSep(ra, dec, 55.5, -30.2) < 0.05, which side is func / radius?
    // We assume the form for <,<=,>,>= since the opposites are invalid, but do additional validation below.
    std::shared_ptr<query::ValueExpr> funcSide;
    std::shared_ptr<query::ValueExpr> radiusSide;
    switch (compPredicate.op) {
        case query::CompPredicate::LESS_THAN_OP:
        case query::CompPredicate::LESS_THAN_OR_EQUALS_OP:
            funcSide = compPredicate.left;
            radiusSide = compPredicate.right;
            break;
        case query::CompPredicate::GREATER_THAN_OP:
        case query::CompPredicate::GREATER_THAN_OR_EQUALS_OP:
            funcSide = compPredicate.right;
            radiusSide = compPredicate.left;
            break;
        case query::CompPredicate::EQUALS_OP:
        case query::CompPredicate::NULL_SAFE_EQUALS_OP:
            // Either form is valid (angSep() == r, r == angSep()). There is a low likelihood that equality
            // with angSep() will be useful, but since it is technically evaluable we support it.
            if (compPredicate.left->isFunction()) {
                funcSide = compPredicate.left;
                radiusSide = compPredicate.right;
            } else {
                funcSide = compPredicate.right;
                radiusSide = compPredicate.left;
            }
            break;
        default:
            return nullptr;
    }

    // Validate the scisql_angSep function and radius
    if (not funcSide->isFunction()) return nullptr;
    auto funcExpr = funcSide->getFunction();
    if (funcExpr->getName() != "scisql_angSep" || funcExpr->getParams().size() != 4) {
        return nullptr;
    }
    auto const& params = funcExpr->getParams();
    double const radius = radiusSide->getNumericConst();
    if (!std::isfinite(radius)) return nullptr;
    if (radius < 0.0) {
        throw qana::AnalysisError("scisql_angSep() comparison threshold must be a non-negative number, got " +
                                  std::to_string(radius));
    }

    // Exactly one of the two coordinate pairs must consist of numeric constants. If neither is, then the
    // predicate is a spatial join and will be handled in qana::RelationGraph.
    bool const first =
            std::isfinite(params[0]->getNumericConst()) && std::isfinite(params[1]->getNumericConst());
    bool const sec =
            std::isfinite(params[2]->getNumericConst()) && std::isfinite(params[3]->getNumericConst());
    if (first == sec) return nullptr;

    // which params are our center of the cone?
    auto const& centerLon = first ? params[0] : params[2];
    auto const& centerLat = first ? params[1] : params[3];

    // which params are our position?
    auto const& posLon = first ? params[2] : params[0];
    auto const& posLat = first ? params[3] : params[1];

    auto const lonColumnRef = posLon->getColumnRef();
    auto const latColumnRef = posLat->getColumnRef();
    if (!lonColumnRef || !latColumnRef) {
        // not plain column refs; can't be used as an area restrictor
        return nullptr;
    }

    if (!isAdmissibleAreaRestrictor(*lonColumnRef, *latColumnRef, context)) {
        LOGS(_log, LOG_LVL_DEBUG,
             "Not using " << funcExpr->getName() << " as an area restrictor; (" << lonColumnRef->getColumn()
                          << ", " << latColumnRef->getColumn() << ") are not the partitioning columns of "
                          << lonColumnRef->getDb() << "." << lonColumnRef->getTable());
        return nullptr;
    }

    return std::make_shared<query::AreaRestrictorCircle>(centerLon->getConstVal(), centerLat->getConstVal(),
                                                         radiusSide->getConstVal());
}

/**
 * @brief If there is exactly one area constraint in the top level AND of the where clause,
 *        return the corresponding area restrictor.
 *
 * @param whereClause WHERE clause to inspect
 * @param context The query context
 * @return std::shared_ptr<query::AreaRestrictor> Area restrictor if there was exactly one
 *         area constraint, else nullptr.
 */
std::shared_ptr<query::AreaRestrictor> extractSingleAreaRestrictor(query::WhereClause const& whereClause,
                                                                   query::QueryContext const& context) {
    auto topLevelAnd = whereClause.getRootAndTerm();
    if (topLevelAnd == nullptr) return nullptr;
    std::shared_ptr<const query::FuncExpr> scisqlFunc;
    std::shared_ptr<query::AreaRestrictor> angSepRestrictor;
    size_t count = 0;  // how many area constraints we find below

    // Scan each top-level AND predicate in the WHERE clause for an area constraint.
    for (auto const& boolTerm : topLevelAnd->_terms) {
        auto boolFactor = std::dynamic_pointer_cast<const query::BoolFactor>(boolTerm);
        if (boolFactor == nullptr) continue;

        // If our BoolFactor has a 'NOT', then it is querying *outside* the region specified, and can't be
        // pruned to a single AreaRestrictor.
        if (boolFactor->_hasNot) continue;

        for (auto boolFactorTerm : boolFactor->_terms) {
            auto compPredicate = std::dynamic_pointer_cast<const query::CompPredicate>(boolFactorTerm);
            if (compPredicate == nullptr) continue;

            // Try to handle this as an angSep. If it's not, we check area functions below.
            auto restrictor = makeAreaRestrictorFromAngSep(*compPredicate, context);
            if (restrictor != nullptr) {
                // Success!
                ++count;
                angSepRestrictor = restrictor;
                continue;
            }

            // Area functions must be of the form scisql_s2Pt*(...) = 1 (or <=> 1):
            if (compPredicate->op != query::CompPredicate::EQUALS_OP &&
                compPredicate->op != query::CompPredicate::NULL_SAFE_EQUALS_OP) {
                continue;
            }

            // Check both sides of the '='
            if (isScisqlAreaFunc(*compPredicate->left) &&
                areaParamsAreConst(*compPredicate->left->getFunction()) &&
                compPredicate->right->getNumericConst() == 1.0) {
                ++count;
                scisqlFunc = compPredicate->left->getFunction();
            }
            if (isScisqlAreaFunc(*compPredicate->right) &&
                areaParamsAreConst(*compPredicate->right->getFunction()) &&
                compPredicate->left->getNumericConst() == 1.0) {
                ++count;
                scisqlFunc = compPredicate->right->getFunction();
            }
        }
    }

    // We only support ONE area restrictor.
    if (count != 1) return nullptr;

    // If this was a scisql_s2PtIn* function, build and return the appropriate class
    if (scisqlFunc != nullptr) return makeAreaRestrictor(*scisqlFunc, context);

    return angSepRestrictor;
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
        // Get the area restrictor if there is exactly one area constraint in the WHERE clause.
        auto areaRestrictor = extractSingleAreaRestrictor(whereClause, context);
        if (areaRestrictor != nullptr) {
            LOGS(_log, LOG_LVL_TRACE, "Adding restrictor: " << *areaRestrictor);
            context.addAreaRestrictors({areaRestrictor});
        }
    }

    handleSecondaryIndex(whereClause, context);
}

void QservRestrictorPlugin::applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context) {
    // Probably nothing is needed here...
}

}  // namespace lsst::qserv::qana
