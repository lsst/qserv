// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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

#include "ccontrol/HyriseAdapter.h"

#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sql/Expr.h"
#include "sql/SelectStatement.h"
#include "sql/Table.h"

#include "ccontrol/HyriseDiagnostics.h"
#include "global/constants.h"
#include "parser/ParseException.h"
#include "query/AndTerm.h"
#include "query/AreaRestrictor.h"
#include "query/BetweenPredicate.h"
#include "query/BoolFactor.h"
#include "query/BoolTermFactor.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/InPredicate.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/LikePredicate.h"
#include "query/NullPredicate.h"
#include "query/OrderByClause.h"
#include "query/OrTerm.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueExprPredicate.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

namespace {

using namespace lsst::qserv;

// Forward declarations
std::shared_ptr<query::AreaRestrictor> buildAreaRestrictor(hsql::Expr const* expr);
std::shared_ptr<query::ValueExpr> buildValueExpr(hsql::Expr const* expr);
std::shared_ptr<query::LogicalTerm> buildBoolTerm(hsql::Expr const* expr);

[[noreturn]] void unsupported(std::string const& what) {
    throw parser::ParseException("HyriseAdapter unsupported SQL construct: " + what);
}

std::string nullToEmpty(char const* value) { return value == nullptr ? std::string() : std::string(value); }

/// Validate identifier string.
/// Currently rejects any identifiers that start with an underscore (reserved by Qserv).
void validateIdentifier(char const* identifier) {
    if (identifier != nullptr && identifier[0] == '_') {
        throw parser::ParseException("Error parsing query, near \"" + nullToEmpty(identifier) +
                                     "\", Identifiers in Qserv may not start with an underscore.");
    }
}

/// Validate the alias name and reject column lists, which Qserv IR cannot represent.
void validateAlias(hsql::Alias const* alias) {
    if (alias == nullptr) return;
    validateIdentifier(alias->name);
    if (alias->columns != nullptr) unsupported("table alias column list");
}

/// Validate an expression node.
/// Currently rejects CASE expressions, which the Qserv IR cannot represent.
void validateExpression(hsql::Expr const* expr) {
    if (expr != nullptr && expr->type == hsql::kExprOperator && expr->opType == hsql::kOpCase) {
        throw parser::ParseException("qserv can not parse query: CASE expressions are not supported.");
    }
}

/// Verify that qserv area restrictors appear only in conjunctive (AND) contexts; throws otherwise.
void validateAreaRestrictorPlacement(hsql::Expr const* expr, bool allowAreaRestrictorExtraction = true) {
    if (expr == nullptr) unsupported("null boolean expression");

    if (buildAreaRestrictor(expr) != nullptr) {
        if (!allowAreaRestrictorExtraction) {
            unsupported("qserv area restrictors are only supported in conjunctive (AND) contexts");
        }
        return;
    }

    if (expr->type == hsql::kExprOperator && (expr->opType == hsql::kOpAnd || expr->opType == hsql::kOpOr)) {
        // Area restrictors are extracted into WhereClause and handled as a special case, effectively making
        // them conjunctive. We explicitly disallow area restrictors combined with OR / NOT.
        auto const childAllowsAreaRestrictorExtraction =
                allowAreaRestrictorExtraction && expr->opType != hsql::kOpOr;
        validateAreaRestrictorPlacement(expr->expr, childAllowsAreaRestrictorExtraction);
        validateAreaRestrictorPlacement(expr->expr2, childAllowsAreaRestrictorExtraction);
        return;
    }

    if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpNot) {
        if (expr->expr == nullptr) unsupported("NOT expression");
        validateAreaRestrictorPlacement(expr->expr, false);
    }
}

/// Return a numeric literal rendered as SQL text, or nullopt if @p expr is not a numeric literal.
std::optional<std::string> numericLiteralToString(hsql::Expr const* expr) {
    if (expr == nullptr) return std::nullopt;
    switch (expr->type) {
        case hsql::kExprLiteralInt:
            return std::to_string(expr->ival);
        case hsql::kExprLiteralIntString:
            return nullToEmpty(expr->name);
        case hsql::kExprLiteralFloatString:
            return nullToEmpty(expr->name);
        default:
            return std::nullopt;
    }
}

/// Map Hyrise arithmetic/bitwise operator to Qserv IR equivalent; throws if unsupported.
query::ValueExpr::Op mapValueOp(hsql::OperatorType op) {
    switch (op) {
        case hsql::kOpPlus:
            return query::ValueExpr::PLUS;
        case hsql::kOpMinus:
            return query::ValueExpr::MINUS;
        case hsql::kOpAsterisk:
            return query::ValueExpr::MULTIPLY;
        case hsql::kOpSlash:
            return query::ValueExpr::DIVIDE;
        case hsql::kOpPercentage:
            return query::ValueExpr::MODULO;
        case hsql::kOpMod:
            return query::ValueExpr::MOD;
        case hsql::kOpDiv:
            return query::ValueExpr::DIV;
        case hsql::kOpCaret:
        case hsql::kOpBitXor:
            return query::ValueExpr::BIT_XOR;
        case hsql::kOpBitAnd:
            return query::ValueExpr::BIT_AND;
        case hsql::kOpBitOr:
            return query::ValueExpr::BIT_OR;
        case hsql::kOpBitShiftLeft:
            return query::ValueExpr::BIT_SHIFT_LEFT;
        case hsql::kOpBitShiftRight:
            return query::ValueExpr::BIT_SHIFT_RIGHT;
        default:
            unsupported("value operator " + ccontrol::HyriseDiagnostics::operatorName(op));
    }
}

/// Map Hyrise comparison operator to Qserv IR equivalent; throws if unsupported.
query::CompPredicate::OpType mapCompOp(hsql::OperatorType op) {
    switch (op) {
        case hsql::kOpEquals:
            return query::CompPredicate::EQUALS_OP;
        case hsql::kOpNullSafeEquals:
            return query::CompPredicate::NULL_SAFE_EQUALS_OP;
        case hsql::kOpNotEquals:
            return query::CompPredicate::NOT_EQUALS_OP;
        case hsql::kOpLess:
            return query::CompPredicate::LESS_THAN_OP;
        case hsql::kOpLessEq:
            return query::CompPredicate::LESS_THAN_OR_EQUALS_OP;
        case hsql::kOpGreater:
            return query::CompPredicate::GREATER_THAN_OP;
        case hsql::kOpGreaterEq:
            return query::CompPredicate::GREATER_THAN_OR_EQUALS_OP;
        default:
            unsupported("comparison operator " + ccontrol::HyriseDiagnostics::operatorName(op));
    }
}

/// Map Hyrise join type to Qserv IR equivalent; throws if unsupported.
query::JoinRef::Type mapJoinType(hsql::JoinType type) {
    switch (type) {
        case hsql::kJoinInner:
            return query::JoinRef::DEFAULT;
        case hsql::kJoinLeft:
            return query::JoinRef::LEFT;
        case hsql::kJoinRight:
            return query::JoinRef::RIGHT;
        case hsql::kJoinFull:
            return query::JoinRef::FULL;
        case hsql::kJoinCross:
            return query::JoinRef::CROSS;
        case hsql::kJoinNatural:
            return query::JoinRef::DEFAULT;
        default:
            unsupported("join type");
    }
}

bool isIntegerLiteral(hsql::Expr const* expr) {
    return expr != nullptr &&
           (expr->type == hsql::kExprLiteralInt || expr->type == hsql::kExprLiteralIntString);
}

template <typename Restrictor>
std::shared_ptr<query::AreaRestrictor> makeAreaRestrictor(std::vector<std::string> const& args) {
    return std::make_shared<Restrictor>(args);
}

using AreaRestrictorFactory = std::shared_ptr<query::AreaRestrictor> (*)(std::vector<std::string> const&);

/// Provides a mapping from supported area restrictor names to their construction to reduce repetitive code.
/// This consolidates (1) listing/testing valid area restrictors, and (2) instantiating them by name.
std::map<std::string, AreaRestrictorFactory> const AREA_RESTRICTOR_FACTORY_MAP = {
        {"qserv_areaspec_box", makeAreaRestrictor<query::AreaRestrictorBox>},
        {"qserv_areaspec_circle", makeAreaRestrictor<query::AreaRestrictorCircle>},
        {"qserv_areaspec_ellipse", makeAreaRestrictor<query::AreaRestrictorEllipse>},
        {"qserv_areaspec_poly", makeAreaRestrictor<query::AreaRestrictorPoly>},
};

/// This map explicitly indicates which aggregation functions are supported by Qserv.
std::map<std::string, bool> const AGGREGATE_SUPPORT_MAP = {
        /* Supported: */
        {"avg", true},
        {"count", true},
        {"max", true},
        {"min", true},
        {"sum", true},

        /* Unsupported: */
        {"bit_and", false},
        {"bit_or", false},
        {"bit_xor", false},
        {"group_concat", false},
        {"json_arrayagg", false},
        {"json_objectagg", false},
        {"std", false},
        {"stddev", false},
        {"stddev_pop", false},
        {"stddev_samp", false},
        {"var_pop", false},
        {"var_samp", false},
        {"variance", false},
};

/// Return true if an expression contains a call to a known aggregate function at any depth.
bool containsAggregateFunction(hsql::Expr const* expr) {
    if (expr == nullptr) return false;
    if (expr->type == hsql::kExprFunctionRef &&
        AGGREGATE_SUPPORT_MAP.contains(boost::algorithm::to_lower_copy(nullToEmpty(expr->name)))) {
        return true;
    }
    if (containsAggregateFunction(expr->expr) || containsAggregateFunction(expr->expr2)) return true;
    if (expr->exprList != nullptr) {
        for (auto const* child : *expr->exprList) {
            if (containsAggregateFunction(child)) return true;
        }
    }
    return false;
}

std::string restrictorArgToString(hsql::Expr const* expr) {
    if (expr == nullptr) unsupported("null qserv area restrictor argument");
    if (auto text = numericLiteralToString(expr)) return *text;
    if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpUnaryMinus) {
        return "-" + restrictorArgToString(expr->expr);
    }
    unsupported("non-constant qserv area restrictor argument");
}

std::shared_ptr<query::ColumnRef> buildColumnRef(hsql::Expr const* expr) {
    if (expr->type != hsql::kExprColumnRef) unsupported("non-column column reference");
    validateIdentifier(expr->schema);
    validateIdentifier(expr->table);
    validateIdentifier(expr->name);
    std::string schema = nullToEmpty(expr->schema);
    std::string table = nullToEmpty(expr->table);
    std::string name = nullToEmpty(expr->name);
    if (name.empty()) unsupported("empty column reference");
    if (!schema.empty() || !table.empty()) {
        return std::make_shared<query::ColumnRef>(schema, table, name);
    }
    return std::make_shared<query::ColumnRef>(name);
}

std::shared_ptr<query::ValueFactor> buildValueFactor(hsql::Expr const* expr) {
    if (expr == nullptr) unsupported("null expression");

    // If this is a numeric literal we can handle it here. Fall through for other types.
    if (auto text = numericLiteralToString(expr)) {
        return query::ValueFactor::newConstFactor(*text);
    }

    switch (expr->type) {
        case hsql::kExprColumnRef:
            return query::ValueFactor::newColumnRefFactor(buildColumnRef(expr));

        case hsql::kExprStar:
            validateIdentifier(expr->table);
            return query::ValueFactor::newStarFactor(nullToEmpty(expr->table));

        case hsql::kExprLiteralString: {
            std::string s = nullToEmpty(expr->name);
            std::string escaped;
            escaped.reserve(s.size());
            for (char c : s) {
                if (c == '\'') escaped += '\'';
                escaped += c;
            }
            return query::ValueFactor::newConstFactor("'" + escaped + "'");
        }

        case hsql::kExprLiteralNull:
            return query::ValueFactor::newConstFactor("NULL");

        case hsql::kExprFunctionRef: {
            validateIdentifier(expr->schema);
            validateIdentifier(expr->name);

            // Filter out disallowed function modifiers
            if (expr->distinct) unsupported("DISTINCT in function arguments");
            if (expr->windowDescription != nullptr) unsupported("window function");
            if (expr->schema != nullptr && expr->schema[0] != '\0') unsupported("schema-qualified function");

            std::string name = nullToEmpty(expr->name);
            auto const nameLower = boost::algorithm::to_lower_copy(name);
            if (AREA_RESTRICTOR_FACTORY_MAP.contains(nameLower))
                unsupported("qserv area restrictor function in this position");

            // Nested aggregations are not allowed (for now)
            if (!AGGREGATE_SUPPORT_MAP.contains(nameLower) && expr->exprList != nullptr) {
                for (auto const* arg : *expr->exprList) {
                    if (containsAggregateFunction(arg)) unsupported("aggregate function in this position");
                }
            }

            // Populate our function and its arguments.
            query::ValueExprPtrVector args;
            if (expr->exprList != nullptr) {
                for (auto const* arg : *expr->exprList) {
                    args.push_back(buildValueExpr(arg));
                }
            }
            auto func = query::FuncExpr::newWithArgs(name, args);

            // Check for supported/unsupported aggregations. Unknown functions/UDFs pass through unchanged.
            if (AGGREGATE_SUPPORT_MAP.contains(nameLower)) {
                if (AGGREGATE_SUPPORT_MAP.at(nameLower) == true) {
                    // Only allow a bare column reference or COUNT(*) for aggregations. This ensures we
                    // maintain previous ANTLR behavior, but should be removed in the future once we expand
                    // aggregation support.
                    if (!(args.size() == 1 &&
                          (args[0]->isColumnRef() || (nameLower == "count" && args[0]->isStar()))))
                        unsupported("aggregate argument must be a column reference");
                    return query::ValueFactor::newAggFactor(func);
                } else {
                    unsupported("aggregate function " + name);
                }
            }

            return query::ValueFactor::newFuncFactor(func);
        }

        case hsql::kExprOperator:
            return query::ValueFactor::newExprFactor(buildValueExpr(expr));

        default:
            unsupported(ccontrol::HyriseDiagnostics::exprTypeName(expr->type));
    }
}

/// Build the single-factor AndTerm for a predicate expression. Predicates that include their own
/// NOT in the IR (IN, BETWEEN, IS NULL) absorb @p negate; for all other predicates it becomes a
/// NOT on the enclosing BoolFactor. LIKE deliberately does not absorb it: `x NOT LIKE y` arrives
/// from the parser as kOpNotLike, while `NOT (x LIKE y)` keeps the outer NOT to match the legacy
/// parser's IR.
std::shared_ptr<query::AndTerm> buildPredicateTerm(hsql::Expr const* expr, bool negate = false) {
    if (expr == nullptr) unsupported("null boolean factor");
    validateExpression(expr);

    std::shared_ptr<query::BoolFactorTerm> predicate;
    if (expr->type != hsql::kExprOperator) {
        predicate = std::make_shared<query::ValueExprPredicate>(buildValueExpr(expr));
    } else if (expr->opType == hsql::kOpIn) {
        if (expr->select != nullptr) unsupported("IN subquery");
        if (expr->exprList == nullptr) unsupported("empty IN list");
        query::ValueExprPtrVector values;
        for (auto const* value : *expr->exprList) {
            values.push_back(buildValueExpr(value));
        }
        predicate = std::make_shared<query::InPredicate>(buildValueExpr(expr->expr), values,
                                                         std::exchange(negate, false));
    } else if (expr->opType == hsql::kOpBetween) {
        if (expr->exprList == nullptr || expr->exprList->size() != 2) unsupported("BETWEEN bounds");
        predicate = std::make_shared<query::BetweenPredicate>(
                buildValueExpr(expr->expr), buildValueExpr(expr->exprList->at(0)),
                buildValueExpr(expr->exprList->at(1)), std::exchange(negate, false));
    } else if (expr->opType == hsql::kOpLike || expr->opType == hsql::kOpNotLike) {
        predicate = std::make_shared<query::LikePredicate>(
                buildValueExpr(expr->expr), buildValueExpr(expr->expr2), expr->opType == hsql::kOpNotLike);
    } else if (expr->opType == hsql::kOpIsNull) {
        predicate = std::make_shared<query::NullPredicate>(buildValueExpr(expr->expr),
                                                           std::exchange(negate, false));
    } else {
        predicate = std::make_shared<query::CompPredicate>(
                buildValueExpr(expr->expr), mapCompOp(expr->opType), buildValueExpr(expr->expr2));
    }

    return std::make_shared<query::AndTerm>(
            query::BoolTerm::PtrVector{std::make_shared<query::BoolFactor>(predicate, negate)});
}

std::shared_ptr<query::AreaRestrictor> buildAreaRestrictor(hsql::Expr const* expr) {
    if (expr == nullptr) return nullptr;

    // The following is intended to match the legacy ANTLR-based parser. It used a dedicated
    // QservFunctionSpec rule for qserv_areaspec_* functions, but only when it was bare or
    // compared against an integer literal ("qserv_areaspec_box(...) = 1", "1 = ...").
    if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpEquals) {
        if (isIntegerLiteral(expr->expr2)) {
            if (auto r = buildAreaRestrictor(expr->expr)) return r;
        }
        if (isIntegerLiteral(expr->expr)) {
            if (auto r = buildAreaRestrictor(expr->expr2)) return r;
        }
    }

    // Ensure this is a function ref, and it does not have a schema qualifier (not supported).
    if (expr->type != hsql::kExprFunctionRef || (expr->schema != nullptr && expr->schema[0] != '\0'))
        return nullptr;

    // Only qserv_areaspec_* are restrictors; for any other function return nullptr so normal value /
    // predicate handling takes over
    std::string name = boost::algorithm::to_lower_copy(nullToEmpty(expr->name));
    if (!AREA_RESTRICTOR_FACTORY_MAP.contains(name)) {
        return nullptr;
    }

    // Set up args
    if (expr->exprList == nullptr) return nullptr;
    std::vector<std::string> args;
    for (auto const* arg : *expr->exprList) {
        args.push_back(restrictorArgToString(arg));
    }

    try {
        // Create the appropriate AreaRestrictor*
        return AREA_RESTRICTOR_FACTORY_MAP.at(name)(args);
    } catch (std::logic_error const& err) {
        throw parser::ParseException(err.what());
    }
}

std::shared_ptr<query::ValueExpr> buildUnaryMinusValueExpr(hsql::Expr const* expr) {
    auto const* operand = expr->expr;
    if (auto text = numericLiteralToString(operand)) {
        return query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("-" + *text));
    }

    auto value = std::make_shared<query::ValueExpr>();
    value->addValueFactor(query::ValueFactor::newConstFactor("0"));
    value->addOp(query::ValueExpr::MINUS);
    value->addValueFactor(buildValueFactor(operand));
    return value;
}

std::shared_ptr<query::ValueExpr> buildValueExpr(hsql::Expr const* expr) {
    if (expr == nullptr) unsupported("null value expression");
    validateExpression(expr);
    if (expr->type != hsql::kExprOperator) {
        return query::ValueExpr::newSimple(buildValueFactor(expr));
    }
    if (expr->opType == hsql::kOpUnaryMinus) {
        return buildUnaryMinusValueExpr(expr);
    }

    auto value = std::make_shared<query::ValueExpr>();
    value->addValueFactor(buildValueFactor(expr->expr));
    value->addOp(mapValueOp(expr->opType));
    value->addValueFactor(buildValueFactor(expr->expr2));
    return value;
}

std::shared_ptr<query::LogicalTerm> buildWhereTerm(hsql::Expr const* expr, query::WhereClause& where) {
    if (expr == nullptr) unsupported("null boolean expression");
    if (auto restrictor = buildAreaRestrictor(expr)) {
        where.addAreaRestrictor(restrictor);
        return nullptr;
    }

    if (expr->type == hsql::kExprOperator && (expr->opType == hsql::kOpAnd || expr->opType == hsql::kOpOr)) {
        auto left = buildWhereTerm(expr->expr, where);
        auto right = buildWhereTerm(expr->expr2, where);

        query::BoolTerm::PtrVector terms;
        if (left != nullptr) terms.push_back(left);
        if (right != nullptr) terms.push_back(right);
        if (terms.empty()) return nullptr;
        if (terms.size() == 1) {
            return left != nullptr ? left : right;
        }
        if (expr->opType == hsql::kOpAnd) {
            auto andTerm = std::make_shared<query::AndTerm>();
            for (auto const& term : terms) {
                if (!andTerm->merge(*term)) {
                    andTerm->addBoolTerm(term);
                }
            }
            return andTerm;
        }

        auto orTerm = std::make_shared<query::OrTerm>();
        for (auto const& term : terms) {
            if (!orTerm->merge(*term)) {
                // OrTerm children are assumed to be AndTerms, but ignore if already an AndTerm
                if (auto andTerm = std::dynamic_pointer_cast<query::AndTerm>(term)) {
                    orTerm->addBoolTerm(andTerm);
                } else {
                    orTerm->addBoolTerm(std::make_shared<query::AndTerm>(term));
                }
            }
        }
        return orTerm;
    }
    return buildBoolTerm(expr);
}

/// Build the LogicalTerm for a boolean expression, handling AND/OR nesting and NOT.
std::shared_ptr<query::LogicalTerm> buildBoolTerm(hsql::Expr const* expr) {
    if (expr == nullptr) unsupported("null boolean expression");
    if (expr->type != hsql::kExprOperator) {
        return buildPredicateTerm(expr);
    }
    if (expr->opType == hsql::kOpAnd || expr->opType == hsql::kOpOr) {
        std::shared_ptr<query::BoolTerm> left = buildBoolTerm(expr->expr);
        if (auto reduced = left->getReduced()) left = reduced;
        std::shared_ptr<query::BoolTerm> right = buildBoolTerm(expr->expr2);
        if (auto reduced = right->getReduced()) right = reduced;
        query::BoolTerm::PtrVector terms{left, right};
        if (expr->opType == hsql::kOpAnd) {
            return std::make_shared<query::AndTerm>(terms);
        }
        return std::make_shared<query::OrTerm>(terms);
    }
    if (expr->opType == hsql::kOpNot) {
        if (expr->expr == nullptr) unsupported("NOT expression");
        if (expr->expr->type == hsql::kExprOperator &&
            (expr->expr->opType == hsql::kOpAnd || expr->expr->opType == hsql::kOpOr)) {
            auto inner = buildBoolTerm(expr->expr);
            auto wrapped =
                    std::make_shared<query::BoolFactor>(std::make_shared<query::BoolTermFactor>(inner), true);
            wrapped->addParenthesis();
            return std::make_shared<query::AndTerm>(query::BoolTerm::PtrVector{wrapped});
        }
        return buildPredicateTerm(expr->expr, true);
    }
    return buildPredicateTerm(expr);
}

std::shared_ptr<query::SelectList> buildSelectList(hsql::SelectStatement const& stmt) {
    auto selectList = std::make_shared<query::SelectList>();
    if (stmt.selectList == nullptr) unsupported("missing select list");
    for (auto const* expr : *stmt.selectList) {
        auto value = buildValueExpr(expr);
        if (expr->alias != nullptr) {
            validateIdentifier(expr->alias);
            value->setAlias(nullToEmpty(expr->alias));
            value->setAliasIsUserDefined(true);
        }
        selectList->addValueExpr(value);
    }
    return selectList;
}

std::shared_ptr<query::JoinSpec> buildJoinSpec(hsql::JoinDefinition const* join) {
    if (join == nullptr) unsupported("missing join definition");
    if (join->namedColumns != nullptr) {
        for (auto const* column : *join->namedColumns) validateIdentifier(column);
        if (join->namedColumns->size() != 1) unsupported("multi-column USING");
        return std::make_shared<query::JoinSpec>(
                std::make_shared<query::ColumnRef>(nullToEmpty(join->namedColumns->front())));
    }
    if (join->condition != nullptr) {
        return std::make_shared<query::JoinSpec>(buildBoolTerm(join->condition));
    }
    if (join->natural || join->type == hsql::kJoinNatural || join->type == hsql::kJoinCross) {
        // natural/cross joins have no specification
        return nullptr;
    }
    unsupported("join without ON or USING");
}

query::TableRef::Ptr buildNamedTableRef(hsql::TableRef const* table) {
    if (table == nullptr) unsupported("missing from table");
    if (table->type != hsql::kTableName) unsupported("non-simple table reference");
    validateIdentifier(table->schema);
    validateIdentifier(table->name);
    validateAlias(table->alias);

    std::string alias;
    if (table->alias != nullptr) alias = nullToEmpty(table->alias->name);

    return std::make_shared<query::TableRef>(nullToEmpty(table->schema), nullToEmpty(table->name), alias);
}

query::TableRef::Ptr buildTableRef(hsql::TableRef const* table) {
    if (table == nullptr) unsupported("missing from table");

    if (table->type == hsql::kTableName) return buildNamedTableRef(table);

    if (table->type != hsql::kTableJoin || table->join == nullptr) {
        unsupported("non-simple table reference");
    }

    // Hyrise represents joins as a tree, e.g.:
    // FROM A
    //   JOIN B ON A.id = B.id
    //   JOIN C ON B.id = C.id
    //   JOIN D ON C.id = D.id
    //
    // Is represented as:
    //   kTableJoin
    //   |- kTableJoin (left)
    //   |  |- kTableJoin (left)
    //   |  |  |- kTableName: 'A' (left)
    //   |  |  |- kTableName: 'B' (right)
    //   |  |- kTableName: 'C' (right)
    //   |- kTableName: 'D' (right)
    //
    // So we traverse down the left side recursively and build the right
    // side on our way up to convert this to a Qserv join list.

    auto left = buildTableRef(table->join->left);

    // Qserv supports only named tables on the right side of an explicit join.
    auto right = buildNamedTableRef(table->join->right);

    left->addJoin(std::make_shared<query::JoinRef>(
            right, mapJoinType(table->join->type),
            table->join->natural || table->join->type == hsql::kJoinNatural, buildJoinSpec(table->join)));
    return left;
}

std::shared_ptr<query::FromList> buildFromList(hsql::SelectStatement const& stmt) {
    auto tables = std::make_shared<query::TableRefList>();
    if (stmt.fromTable == nullptr) unsupported("missing FROM");
    if (stmt.fromTable->type == hsql::kTableCrossProduct) {
        // We are selecting from multiple tables e.g., SELECT * FROM Object, Source, Filter
        // (cross-product / CROSS JOIN)
        if (stmt.fromTable->list == nullptr) unsupported("empty table list");
        for (auto const* table : *stmt.fromTable->list) {
            tables->push_back(buildTableRef(table));
        }
    } else {
        // single FROM
        tables->push_back(buildTableRef(stmt.fromTable));
    }
    return std::make_shared<query::FromList>(tables);
}

std::shared_ptr<query::WhereClause> buildWhereClause(hsql::SelectStatement const& stmt) {
    if (stmt.whereClause == nullptr) return nullptr;

    // Qserv area restrictors have specific placement requirements:
    validateAreaRestrictorPlacement(stmt.whereClause);

    // The parse tree for WHERE works similarly to table refs, see buildTableRefs
    // for a brief explanation.
    auto where = std::make_shared<query::WhereClause>();
    auto rootTerm = buildWhereTerm(stmt.whereClause, *where);
    if (rootTerm != nullptr) {
        where->setRootTerm(rootTerm);
    }
    return where;
}

std::shared_ptr<query::OrderByClause> buildOrderBy(hsql::SelectStatement const& stmt) {
    if (stmt.order == nullptr || stmt.order->empty()) return nullptr;

    auto orderBy = std::make_shared<query::OrderByClause>();
    for (auto const* term : *stmt.order) {
        if (term->null_ordering != hsql::NullOrdering::Undefined) unsupported("NULLS FIRST/LAST in ORDER BY");

        auto valueExpr = buildValueExpr(term->expr);
        if (valueExpr->isFunction()) {
            throw parser::ParseException(
                    "qserv does not support functions in ORDER BY. Select the expression under an alias "
                    "and order by the alias instead, e.g. \"SELECT ..., f(x) AS fx ... ORDER BY fx\".");
        }

        query::OrderByTerm::Order order = query::OrderByTerm::DEFAULT;
        if (term->type == hsql::kOrderAsc) order = query::OrderByTerm::ASC;
        if (term->type == hsql::kOrderDesc) order = query::OrderByTerm::DESC;

        orderBy->addTerm(query::OrderByTerm(valueExpr, order));
    }
    return orderBy;
}

std::shared_ptr<query::GroupByClause> buildGroupBy(hsql::SelectStatement const& stmt) {
    if (stmt.groupBy == nullptr || stmt.groupBy->columns == nullptr || stmt.groupBy->columns->empty()) {
        return nullptr;
    }

    auto groupBy = std::make_shared<query::GroupByClause>();
    for (auto const* expr : *stmt.groupBy->columns) {
        groupBy->addTerm(query::GroupByTerm(buildValueExpr(expr), std::string()));
    }
    return groupBy;
}

std::shared_ptr<query::HavingClause> buildHaving(hsql::SelectStatement const& stmt) {
    if (stmt.groupBy != nullptr && stmt.groupBy->having != nullptr) {
        return std::make_shared<query::HavingClause>(buildBoolTerm(stmt.groupBy->having));
    }
    if (stmt.having != nullptr) {
        return std::make_shared<query::HavingClause>(buildBoolTerm(stmt.having));
    }
    return nullptr;
}

/// Return the LIMIT value, or NOTSET when absent. Throws on OFFSET and non-integer limits.
int buildLimit(hsql::SelectStatement const& stmt) {
    if (stmt.limit == nullptr || stmt.limit->limit == nullptr) return lsst::qserv::NOTSET;
    if (stmt.limit->offset != nullptr) unsupported("OFFSET");

    auto const* limit = stmt.limit->limit;
    if (limit->type != hsql::kExprLiteralInt) unsupported("non-integer LIMIT");
    if (limit->ival > static_cast<int64_t>(std::numeric_limits<int>::max())) unsupported("LIMIT overflow");

    return static_cast<int>(limit->ival);
}

}  // namespace

namespace lsst::qserv::ccontrol {

std::shared_ptr<query::SelectStmt> HyriseAdapter::makeSelectStmt(std::string const& sql) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(sql, &result);

    if (!result.isValid() || result.size() == 0) {
        throw parser::ParseException("Failed to instantiate query: \"" + sql + '"');
    }

    if (result.size() > 1) {
        unsupported("multiple statements");
    }

    auto const* stmt = dynamic_cast<hsql::SelectStatement const*>(result.getStatement(0));

    if (stmt == nullptr) unsupported("non-SELECT statement");
    if (stmt->setOperations != nullptr && !stmt->setOperations->empty()) unsupported("set operations");
    if (stmt->withDescriptions != nullptr && !stmt->withDescriptions->empty()) unsupported("WITH");
    if (stmt->lockings != nullptr && !stmt->lockings->empty()) unsupported("row locking");

    return std::make_shared<query::SelectStmt>(
            buildSelectList(*stmt), buildFromList(*stmt), buildWhereClause(*stmt), buildOrderBy(*stmt),
            buildGroupBy(*stmt), buildHaving(*stmt), stmt->selectDistinct, buildLimit(*stmt));
}

}  // namespace lsst::qserv::ccontrol
